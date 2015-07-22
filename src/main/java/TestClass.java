import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class TestClass {
    private static final int MEM_BUF_SIZE = (1 * 1024 * 1024) >> 2;

    //private static final int FILE_INT_SIZE = (1 * 1024 * 1024 * 1024) >>2;
    private static final int FILE_INT_SIZE = (64 * 1024 * 1024) >>2;
    //private static final int FILE_INT_SIZE = (16 * 1024) >>2;

    private static ByteBuffer bb = ByteBuffer.wrap(new byte[MEM_BUF_SIZE]).order(ByteOrder.BIG_ENDIAN);

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("d:/test.out", "rw");
        FileChannel fc = raf.getChannel();

        try {
            IntBuffer ib = bb.asIntBuffer();
            int size = 0;
            for (int i=0; i<FILE_INT_SIZE; i++) {
                if (!bb.hasRemaining()) {
                    bb.flip();
                    size = fc.write(bb);
                    bb.clear();
                }
                bb.putInt(Integer.MAX_VALUE - i);
            }
            bb.flip();
            size = fc.write(bb);
            fc.truncate(FILE_INT_SIZE<<2);
            long f = System.currentTimeMillis() - s;
            System.out.printf("File generated in %s min %.2f sec.", f / 1000 / 60, f / 1000.0);
        } finally {
            fc.close();
        }

    }
}
