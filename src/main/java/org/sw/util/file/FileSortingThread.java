package org.sw.util.file;

import org.sw.util.array.ArraySorter;
import org.sw.util.array.IntByteUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main sorting thread. Provides facility to sort a file in a parallel way.
 * <p/>
 * File is sorted using a merge-sort approach. Result of each step (mergeSort or Sort) is
 * written back into the original file.
 * MergeSort is performed through a temporary file which is removed after operation.
 */
public class FileSortingThread extends Thread {

    //In DEBUG MODE all temporary files will be kept
    public static boolean DEBUG_MODE = false;

    //If set to true main buffer used for sorting is direct
    public static boolean DIRECT_SORT_BUFFER = false;
    public static boolean DIRECT_MERGE_BUFFER = false;

    public static int SORTBUFF_INT_SIZE = (1 * 1024 * 1024) >> 2;
    public static int MERGEBUFF_INT_SIZE = (8 * 1024) >> 2;

    ByteBuffer readBuff1;
    ByteBuffer readBuff2;
    ByteBuffer mergeBuff;

    {
        if (DIRECT_MERGE_BUFFER) {
            readBuff1 = ByteBuffer.allocateDirect(MERGEBUFF_INT_SIZE << 2).order(ByteOrder.BIG_ENDIAN);
            readBuff2 = ByteBuffer.allocateDirect(MERGEBUFF_INT_SIZE << 2).order(ByteOrder.BIG_ENDIAN);
            mergeBuff = ByteBuffer.allocateDirect(MERGEBUFF_INT_SIZE << 2).order(ByteOrder.BIG_ENDIAN);
        } else {
            readBuff1 = ByteBuffer.wrap(new byte[MERGEBUFF_INT_SIZE << 2]).order(ByteOrder.BIG_ENDIAN);
            readBuff2 = ByteBuffer.wrap(new byte[MERGEBUFF_INT_SIZE << 2]).order(ByteOrder.BIG_ENDIAN);
            mergeBuff = ByteBuffer.wrap(new byte[MERGEBUFF_INT_SIZE << 2]).order(ByteOrder.BIG_ENDIAN);
        }
    }

    private ByteBuffer bb;
    private int[] intBuff;

    {
        if (DIRECT_SORT_BUFFER) {
            bb = ByteBuffer.allocateDirect(SORTBUFF_INT_SIZE << 1).order(ByteOrder.BIG_ENDIAN);
            intBuff = new int[SORTBUFF_INT_SIZE << 1];
        } else {
            bb = ByteBuffer.wrap(new byte[SORTBUFF_INT_SIZE << 2]).order(ByteOrder.BIG_ENDIAN);
        }
    }


    private final AtomicInteger threadsAvailable;
    private String filename;
    private long startPos;
    private long endPos;
    private int maxThreadDepth;
    private int curdepth;

    private IOException e = null;

    /**
     * Creates a sorting thread instance.
     * <p/>
     * After creation FileSortingThread#mergeSort() should be launched.
     *
     * @param threadsAvailable - number of threads allowed to be launched,
     * @param filename         - file to be sorted
     * @param startPos         - start index (of integer value in a file)
     * @param endPos           - end index (of integer value in a file)
     */
    public FileSortingThread(AtomicInteger threadsAvailable, String filename, long startPos, long endPos, int maxThreadDepth, int curdepth) {
        this.threadsAvailable = threadsAvailable;
        this.filename = filename;
        this.startPos = startPos;
        this.endPos = endPos;
        this.maxThreadDepth = maxThreadDepth;
        this.curdepth = curdepth;
        setDaemon(true);
        setName("SortingThread_" + curdepth + "_" + startPos + "_" + endPos);
    }

    public IOException getEx() {
        return e;
    }

    @Override
    public void run() {
        File file = new File(filename);
        try {
            mergeSort(filename, startPos, endPos, file.getParentFile(), curdepth);
        } catch (IOException ex) {
            ex.printStackTrace();
            this.e = ex;
        }

    }

    public void mergeSort() throws IOException {
        File parentDir = new File(filename).getParentFile();
        mergeSort(filename, startPos, endPos, parentDir, curdepth);
    }

    /**
     * @param filename
     * @param startPos
     * @param endPos
     * @param tempFileDir - Directory where temporary files will be created.
     * @return
     * @throws IOException
     */
    private void mergeSort(String filename, long startPos, long endPos, File tempFileDir, int depth) throws IOException {
        boolean result = true;

        long length = endPos - startPos + 1;
        if (length <= 1) return;
        if (DIRECT_SORT_BUFFER ? (length <= SORTBUFF_INT_SIZE >> 1) : (length <= SORTBUFF_INT_SIZE)) {
            sortSegment(filename, startPos, endPos);
            return;
        }
        long mid = length / 2;
        if (depth < maxThreadDepth && threadsAvailable.decrementAndGet() > 0) {
            //System.out.println("Thread create, depth=" + depth);
            FileSortingThread fst = new FileSortingThread(threadsAvailable, filename, startPos, startPos + mid - 1, maxThreadDepth, depth + 1);
            fst.start();
            mergeSort(filename, startPos + mid, endPos, tempFileDir, depth + 1);
            try {
                fst.join();
                if (fst.getEx() != null) {
                    throw fst.getEx();
                }
            } catch (InterruptedException e) {
                System.out.println("Upss... interrupted. Sorry.");
                throw new RuntimeException("Sorting interrupted.");
            }
        } else {
            mergeSort(filename, startPos, startPos + mid - 1, tempFileDir, depth + 1);
            mergeSort(filename, startPos + mid, endPos, tempFileDir, depth + 1);
        }
        merge(filename, startPos, endPos, tempFileDir);
    }

    private void sortSegment(String filename, long startPos, long endPos) throws IOException {
        FileChannel fc = FileUtil.getRAFile(filename).getChannel();
        try {
            bb.clear();
            long pos = IntByteUtil.byteIdx(startPos);
            while (pos <= IntByteUtil.byteIdx(endPos)) {
                pos += fc.read(bb, pos);
            }
            bb.flip();
            if (!DIRECT_SORT_BUFFER) {
                ArraySorter.sortBigEndianInts(bb.array(), 0, (int) (endPos - startPos + 1));
                if (DEBUG_MODE) {
                    FileUtil.storeBuffToFileDebug("sort_" + startPos + "_" + endPos + "_", new File(filename).getParentFile(), bb);
                }
                bb.position((int) IntByteUtil.byteIdx((endPos - startPos + 1)));
            } else {
                //For direct buffer only since you cannot access array under ByteBuffer
                IntBuffer ib = bb.asIntBuffer();
                ib.get(intBuff, 0, (int) (endPos - startPos + 1));
                Arrays.sort(intBuff, 0, (int) (endPos - startPos + 1));
                ib.clear();
                bb.clear();
                ib.put(intBuff, 0, (int) (endPos - startPos + 1));
                bb.position(ib.position() << 2);
            }
            FileUtil.flushBuff(fc, IntByteUtil.byteIdx(startPos), bb);
        } finally {
            fc.close();
        }
    }

    private void merge(String filename, long startPos, long endPos, File parentDir) throws IOException {
        FileChannel fc = FileUtil.getRAFile(filename).getChannel();
        File tempFile = FileUtil.getTempFile("merge_" + startPos + "_" + endPos + "_", parentDir);
        RandomAccessFile raf = FileUtil.getRAFile(tempFile);
        FileChannel tempFc = raf.getChannel();
        try {
            long mid = (endPos - startPos + 1) / 2;
            Reader reader = Merger.mergedReader(FileUtil.getRAFile(filename).getChannel(),
                    FileUtil.getRAFile(filename).getChannel(),
                    IntByteUtil.byteIdx(startPos),
                    IntByteUtil.byteIdx(startPos + mid),
                    IntByteUtil.byteIdx(startPos + mid - 1),
                    IntByteUtil.byteIdx(endPos),
                    readBuff1,
                    readBuff2);
            long pos = 0;
            mergeBuff.clear();
            while (reader.hasNext()) {
                int curInt = reader.readNext();
                mergeBuff.putInt(curInt);
                pos = FileUtil.flushBuffIfFull(tempFc, pos, mergeBuff);
            }
            FileUtil.flushBuff(tempFc, pos, mergeBuff);

            FileUtil.transfer(fc, tempFc, IntByteUtil.byteIdx(startPos), 0, mergeBuff, IntByteUtil.byteIdx(endPos - startPos + 1));
        } finally {
            fc.close();
            tempFc.close();
            if (!DEBUG_MODE) {
                tempFile.delete();
            }
        }
    }
}
