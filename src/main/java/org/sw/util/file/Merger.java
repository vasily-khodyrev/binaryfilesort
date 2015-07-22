package org.sw.util.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Provides a reader facility for two file channels.
 */
public class Merger {
    /**
     *
     * Provides a reader facility for two file channels.
     *
     * @param fc1 - First source file channel
     * @param fc2 - Second source file channel
     * @param s1 - startIndex in the first channel
     * @param s2 - startIndex in the second channel
     * @param e1 - endIndex in the first channel
     * @param e2 - endIndex in the second channel
     * @param a - buffer to be used for the first channel
     * @param b - buffer to be used for the second channel
     * @return Reader instance
     */
    public static Reader mergedReader(FileChannel fc1, FileChannel fc2, long s1,long s2, long e1, long e2, ByteBuffer a, ByteBuffer b) {
        return new MergedReader(fc1, fc2, s1,s2,e1,e2, a, b);
    }

    protected static class MergedReader implements Reader {
        protected final FileChannelReader fcr1;
        protected final FileChannelReader fcr2;


        protected int a;
        protected boolean hasA;
        protected int b;
        protected boolean hasB;

        private long counter;

        public MergedReader(FileChannel fc1, FileChannel fc2, long s1,long s2, long e1, long e2, ByteBuffer a, ByteBuffer b) {

            fcr1 = new FileChannelReader(fc1,a,s1,e1);
            fcr2 = new FileChannelReader(fc2,b,s2,e2);
        }

        @Override
        public long getCounter() {
            return counter;
        }

        @Override
        public int readNext() throws IOException {
            counter++;
            if (!hasA&&!fcr1.hasRemaining()) {
                if (!hasB&&!fcr2.hasRemaining()) {
                    throw new IllegalStateException("There's nothing to read.");
                }
                int result;
                if (hasB) {
                    result = b;
                    hasB=false;
                } else {
                  result = fcr2.getInt();
                }
                return result;
            }
            if (!hasB&&!fcr2.hasRemaining()) {
                int result;
                if (hasA) {
                    result = a;
                    hasA=false;
                } else {
                    result = fcr1.getInt();
                }
                return result;
            }

            int result;
            if (!hasA) {
                a=fcr1.getInt();
                hasA=true;
            }
            if (!hasB) {
                b=fcr2.getInt();
                hasB=true;
            }
            if (a<=b) {
                result = a;
                hasA=false;
            } else {
                result = b;
                hasB = false;
            }
            return result;
        }

        @Override
        public boolean hasNext()  throws IOException {
            return hasA || hasB || fcr1.hasRemaining()||fcr2.hasRemaining();
        }
    }
}


