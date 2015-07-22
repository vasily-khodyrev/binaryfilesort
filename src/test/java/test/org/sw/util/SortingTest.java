package test.org.sw.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sw.util.array.IntByteUtil;
import org.sw.util.file.FileSortingThread;
import org.sw.util.file.FileUtil;
import org.sw.util.timer.StatisticsTimer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SortingTest {
    private static int MEM_BUF_SIZE = 8 * 1024;
    private static ByteBuffer bb = ByteBuffer.allocateDirect(MEM_BUF_SIZE).order(ByteOrder.BIG_ENDIAN);
    private static final int FILE_INT_SIZE = (16 * 1024 * 1024);
    private static final int FILE_1GB_INT_SIZE = (1 * 1024 * 1024 * 1024) >> 2;
    private static final String FILENAME = "d:/test.out";

    private static final int FILE_CUSTOM_INT_SIZE = (1 * 1024 * 1024) >> 2;
    private static final int SORTBUFF_CUSTOM_INT_SIZE = (1 * 1024) >> 2;
    private static final int MERGEBUFF_CUSTOM_INT_SIZE = (1 * 1024) >> 2;


    private static final int MAX_THREADS = 4;
    private static final int TEST_RUNS = 5;

    @Before
    public void init() {
        try {
            createDescendSortedFile(FILENAME, FILE_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
    }

    @Test
    public void testMultiThreadSort() {
        try {
            FileSortingThread.DIRECT_SORT_BUFFER=false;
            FileSortingThread.DIRECT_MERGE_BUFFER=true;
            for (int k = 1; k <= MAX_THREADS; k++) {
                int nThreads = k;

                for (int i = 0; i < TEST_RUNS; i++) {
                    createDescendSortedFile(FILENAME + i, FILE_INT_SIZE, bb);
                }
                System.out.println("Sorting in " + k + " thread(s)...");
                StatisticsTimer t = new StatisticsTimer();
                for (int i = 0; i < TEST_RUNS; i++) {
                    FileSortingThread fst = new FileSortingThread(new AtomicInteger(nThreads), FILENAME + i, 0, FILE_INT_SIZE - 1, IntByteUtil.getMax2Power(nThreads), 0);
                    t.start();
                    fst.mergeSort();
                    t.stop().print("sort run = " + i);
                    Assert.assertTrue("File is not sorted!", isSorted(FILENAME + i, bb));
                    new File(FILENAME + i).delete();
                }
                t.printStat("stat filesort size=" + (FILE_INT_SIZE >> 8) + "kB" + " nthreads=" + nThreads);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("File sorting failed");
        }
    }

    @Test
    @Ignore
    public void test1GbFileSort() {
        //FileSortingThread.DEBUG_MODE = true;
        System.out.println("Generating 1Gb file...");
        try {
            createDescendSortedFile(FILENAME, FILE_1GB_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
        System.out.println("Generation completed.");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            FileSortingThread fst = new FileSortingThread(new AtomicInteger(1), FILENAME, 0, FILE_1GB_INT_SIZE - 1, IntByteUtil.getMax2Power(1), 0);
            StatisticsTimer t = new StatisticsTimer();
            t.start();
            fst.mergeSort();
            t.stop().print("File sorted.");
            System.out.println("Verifying the result...");
            Assert.assertTrue("File is not sorted!", isSorted(FILENAME, bb));
            System.out.println("Verifying completed.");
            Assert.assertTrue("Late sorting of 1GB: more than 15 min", t.getTime() < 15 * 60 * 1000);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("File sorting failed");
        }
    }

    @Test
    public void testCustomParamsFileSort() {
        System.out.println("Generating " + (FILE_CUSTOM_INT_SIZE >> 8) + "kB file...");
        try {
            createDescendSortedFile(FILENAME, FILE_CUSTOM_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
        System.out.println("Generation completed.");
        System.out.println("Sorting SORTBUFF_INT_SIZE=" + (SORTBUFF_CUSTOM_INT_SIZE >> 8) + "kB MERGEBUFF_INT_SIZE=" + (MERGEBUFF_CUSTOM_INT_SIZE >> 8) + "kB");
        try {
            FileSortingThread.SORTBUFF_INT_SIZE = SORTBUFF_CUSTOM_INT_SIZE;
            FileSortingThread.MERGEBUFF_INT_SIZE = MERGEBUFF_CUSTOM_INT_SIZE;
            FileSortingThread fst = new FileSortingThread(new AtomicInteger(1), FILENAME, 0, FILE_CUSTOM_INT_SIZE - 1, IntByteUtil.getMax2Power(1), 0);
            StatisticsTimer t = new StatisticsTimer();
            t.start();
            fst.mergeSort();
            t.stop().print("File sorted.");
            System.out.println("Verifying the result...");
            Assert.assertTrue("File is not sorted!", isSorted(FILENAME, bb));
            System.out.println("Verifying completed.");
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("File sorting failed");
        }
    }

    @Test
    public void testFileCreation() {
        try {
            createDescendSortedFile(FILENAME, FILE_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
    }

    @Test
    public void testFileMultiThreadRead() throws IOException {
        try {
            createDescendSortedFile(FILENAME, FILE_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
        MEM_BUF_SIZE = 16 * 1024;

        ExecutorService ss = Executors.newSingleThreadExecutor();
        CountDownLatch clSingle = new CountDownLatch(2);
        DummyReader r = new DummyReader(clSingle, FILENAME, 0, FILE_INT_SIZE - 1);

        StatisticsTimer t1 = new StatisticsTimer();
        try {
            Future f = ss.submit(r);
            clSingle.countDown();
            clSingle.await();
            t1.start();
            f.get();
            t1.stop().print("" + (FILE_INT_SIZE << 2) + " Bytes read");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        for (int NPARTS = 2; NPARTS < Runtime.getRuntime().availableProcessors() + 2; NPARTS++) {
            ExecutorService es = Executors.newFixedThreadPool(NPARTS);
            System.out.println("NPARTS = " + NPARTS);
            CountDownLatch cl = new CountDownLatch(NPARTS + 1);
            ArrayList<DummyReader> dl = new ArrayList<DummyReader>();
            int part = FILE_INT_SIZE / NPARTS;
            for (int i = 0; i < NPARTS; i++) {
                dl.add(new DummyReader(cl, FILENAME, part * i, part * (i + 1) - 1));
            }

            StatisticsTimer t2 = new StatisticsTimer();
            try {
                ArrayList<Future> fl = new ArrayList<Future>();
                for (DummyReader dr : dl) {
                    fl.add(es.submit(dr));
                }
                cl.countDown();
                cl.await();
                t2.start();
                for (Future f : fl) {
                    f.get();
                }
                t2.stop().print("" + (FILE_INT_SIZE << 2) + " Bytes read");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testFileMultiThreadWrite() throws IOException {
        try {
            createDescendSortedFile(FILENAME, FILE_INT_SIZE, bb);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Initial file for sorting cannot be created");
        }
        MEM_BUF_SIZE = 16*1024;

        ExecutorService ss = Executors.newSingleThreadExecutor();
        CountDownLatch clSingle = new CountDownLatch(2);
        DummyWriter r = new DummyWriter(clSingle, FILENAME, 0, FILE_INT_SIZE - 1);

        StatisticsTimer t1 = new StatisticsTimer();
        try {
            Future f = ss.submit(r);
            clSingle.countDown();
            clSingle.await();
            t1.start();
            f.get();
            t1.stop().print("" + (FILE_INT_SIZE << 2) + " Bytes written");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        for (int NPARTS = 2; NPARTS < Runtime.getRuntime().availableProcessors() + 2; NPARTS++) {
            ExecutorService es = Executors.newFixedThreadPool(NPARTS);
            System.out.println("NPARTS = " + NPARTS);
            CountDownLatch cl = new CountDownLatch(NPARTS + 1);
            ArrayList<DummyWriter> dl = new ArrayList<DummyWriter>();
            int part = FILE_INT_SIZE / NPARTS;
            for (int i = 0; i < NPARTS; i++) {
                dl.add(new DummyWriter(cl, FILENAME, part * i, part * (i + 1) - 1));
            }

            StatisticsTimer t2 = new StatisticsTimer();
            try {
                ArrayList<Future> fl = new ArrayList<Future>();
                for (DummyWriter dr : dl) {
                    fl.add(es.submit(dr));
                }
                cl.countDown();
                cl.await();
                t2.start();
                for (Future f : fl) {
                    f.get();
                }
                t2.stop().print("" + (FILE_INT_SIZE << 2) + " Bytes written");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isSorted(String filename, ByteBuffer bb) throws IOException {
        boolean result = true;
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        FileChannel fc = raf.getChannel();
        try {
            bb.clear();
            long pos = 0;
            long size = fc.size();
            int curVal = Integer.MIN_VALUE;
            while (pos < size) {
                pos += fc.read(bb, pos);
                bb.flip();
                IntBuffer ib = bb.asIntBuffer();
                for (int i = 0; ib.hasRemaining(); i++) {
                    int val = ib.get();
                    if (val > curVal) {
                        curVal = val;
                    } else {
                        System.out.println("Unordered integer found: position = " + pos + " value = " + val);
                        return false;
                    }
                }
                bb.clear();
            }
        } finally {
            fc.close();
        }
        return result;
    }

    private static void createDescendSortedFile(String filename, int filesize, ByteBuffer bb) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filename, "rw");
        FileChannel fc = raf.getChannel();
        bb.clear();
        try {
            for (int i = 0; i < filesize; i++) {
                if (!bb.hasRemaining()) {
                    bb.flip();
                    fc.write(bb);
                    bb.clear();
                }
                bb.putInt(Integer.MAX_VALUE - i);
            }
            bb.flip();
            fc.write(bb);
            fc.truncate(filesize << 2);
        } finally {
            fc.close();
        }
    }

    public static class DummyReader implements Runnable {
        //ByteBuffer bb = ByteBuffer.wrap(new byte[MEM_BUF_SIZE]).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer bb = ByteBuffer.allocateDirect(MEM_BUF_SIZE).order(ByteOrder.BIG_ENDIAN);
        private String filename;
        private long startPos;
        private long endPos;
        private CountDownLatch cl;
        private StatisticsTimer timer = new StatisticsTimer();

        public DummyReader(CountDownLatch cl, String filename, long startPos, long endPos) {
            this.filename = filename;
            this.startPos = IntByteUtil.byteIdx(startPos);
            this.endPos = IntByteUtil.byteIdx(endPos);
            this.cl = cl;
        }

        @Override
        public void run() {
            try {
                FileChannel fc = FileUtil.getRAFile(filename).getChannel();
                try {
                    cl.countDown();
                    try {
                        cl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    timer.start();
                    bb.clear();
                    long pos = startPos;
                    while (pos < endPos) {
                        if (!bb.hasRemaining()) bb.clear();
                        pos += fc.read(bb, pos);
                    }
                } finally {
                    fc.close();
                    timer.stop();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public StatisticsTimer getTimer() {
            return timer;
        }
    }

    public static class DummyWriter implements Runnable {
        //ByteBuffer bb = ByteBuffer.wrap(new byte[MEM_BUF_SIZE]).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer bb = ByteBuffer.allocateDirect(MEM_BUF_SIZE).order(ByteOrder.BIG_ENDIAN);
        private String filename;
        private long startPos;
        private long endPos;
        private CountDownLatch cl;
        private StatisticsTimer timer = new StatisticsTimer();

        public DummyWriter(CountDownLatch cl, String filename, long startPos, long endPos) {
            this.filename = filename;
            this.startPos = IntByteUtil.byteIdx(startPos);
            this.endPos = IntByteUtil.byteIdx(endPos);
            this.cl = cl;
        }

        @Override
        public void run() {
            try {
                FileChannel fc = FileUtil.getRAFile(filename).getChannel();
                try {
                    cl.countDown();
                    try {
                        cl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    timer.start();
                    bb.clear();
                    long pos = startPos;
                    while (pos < endPos) {
                        if (!bb.hasRemaining()) bb.clear();
                        pos += fc.write(bb, pos);
                    }
                } finally {
                    fc.close();
                    timer.stop();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public StatisticsTimer getTimer() {
            return timer;
        }
    }
}
