package org.sw.util.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

public class FileUtil {

    public static RandomAccessFile getRAFile(File file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        return raf;
    }

    public static RandomAccessFile getRAFile(String filename) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filename, "rw");
        return raf;
    }

    public static File getTempFile(String prefix, File parent) throws IOException {

        File tempFile = File.createTempFile(prefix, null, parent);
        tempFile.deleteOnExit();
        return tempFile;
    }

    public static FileChannel getTempFc(String name, File parent, boolean debugMode) throws IOException {
        File tempFile = File.createTempFile(name, null, parent);
        if (!debugMode) {
            tempFile.deleteOnExit();
        }
        return getRAFile(tempFile).getChannel();
    }

    public static void storeBuffToFileDebug(String prefix, File parent, ByteBuffer bb) throws IOException {
        FileChannel fc = getTempFc(prefix, parent, true);
        try {
            while (bb.hasRemaining()) {
                fc.write(bb);
            }
        } finally {
            fc.close();
        }
    }

    public static void storeBuffToFile(String filename, long dstPos, ByteBuffer bb) throws IOException {
        FileChannel fc = getRAFile(filename).getChannel();
        try {
            while (bb.hasRemaining()) {
                fc.write(bb);
            }
        } finally {
            fc.close();
        }
    }

    /**
     * Transfers a number (length) of integer content of on FileChannel (from srcPos) to another FileChannel (at dstPos).
     *
     * @param fcDst  - Destination file channel
     * @param fcSrc  - Source file channel
     * @param dstPos - position in destination file
     * @param srcPos - position in source file
     * @param bb - used ByteBuffer
     * @param length - number of integers to transfer
     * @throws IOException
     */
    public static void transfer(FileChannel fcDst, FileChannel fcSrc, long dstPos, long srcPos, ByteBuffer bb, long length) throws IOException {
        long srcEndPos = srcPos + length;
        long dstEndPos = dstPos + length;
        bb.clear();
        while (dstPos < dstEndPos) {
            if (bb.hasRemaining()) {
                srcPos += fcSrc.read(bb, srcPos);
            }
            if (srcPos > srcEndPos) {
                bb.position(bb.position() - (int) (srcPos - srcEndPos));
            }
            bb.flip();
            while (bb.hasRemaining()) {
                dstPos += fcDst.write(bb, dstPos);
            }
            bb.clear();
        }
    }

    public static long flushBuffIfFull(FileChannel fc, long bytePos, ByteBuffer bb) throws IOException {
        if (!bb.hasRemaining()) {
            return flushBuff(fc, bytePos, bb);
        }
        return bytePos;
    }

    public static long flushBuff(FileChannel fc, long pos, ByteBuffer bb) throws IOException {
        long curPos = pos;
        bb.flip();
        while (bb.hasRemaining()) {
            curPos += fc.write(bb, curPos);
        }
        bb.rewind();
        return curPos;
    }


}
