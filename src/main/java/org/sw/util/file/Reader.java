package org.sw.util.file;

import java.io.IOException;

public interface Reader {

    public int readNext() throws IOException;

    public boolean hasNext() throws IOException;

    public long getCounter();
}
