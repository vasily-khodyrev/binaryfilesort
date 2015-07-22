package org.sw.util.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelReader {
    private FileChannel fc;
    private ByteBuffer bb;
    private long endPos;
    private long curPos;
    protected int a;
    protected boolean hasA;

    public FileChannelReader(FileChannel fc, ByteBuffer bb, long startPos, long endPos) {
        this.fc = fc;
        this.bb = bb;
        this.curPos = startPos;
        this.endPos = endPos;
        this.bb.limit(0);
    }

    /**
     * Get next integer value
     *
     * @return next integer value
     * @throws IOException If some I/O error occurs ()
     */
    public int getInt() throws IOException {
        if (hasRemaining()) {
            hasA = false;
            return a;
        } else {
            throw new IllegalStateException("No more data to provide");
        }

    }

    /**
     *
     * @return true if there're some values to read.
     * @throws IOException  If some I/O error occurs (@see FileChannel#read)
     */
    public boolean hasRemaining() throws IOException {
        if (hasA) {
            return true;
        }
        if (bb.hasRemaining() && bb.remaining()>=4) {
            a = bb.getInt();
            hasA = true;
            return true;
        } else {
            if (curPos < endPos+4) {
                bb.clear();
                while (bb.hasRemaining() && curPos < endPos+4) {
                    curPos += fc.read(bb, curPos);
                }
                if (curPos > endPos+4) {
                    bb.position(bb.position()-(int)(curPos-endPos-4));
                }
                bb.flip();
                a = bb.getInt();
                hasA = true;
                return true;
            }

        }
        return false;
    }
}
