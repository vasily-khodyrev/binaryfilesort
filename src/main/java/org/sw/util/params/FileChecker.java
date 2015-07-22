package org.sw.util.params;

import java.io.File;

public class FileChecker {
    public static boolean isValid(String filename) {
        boolean result = false;
        File file = null;
        if (filename != null) {
            file = new File(filename);
            if (file.isFile() && file.canRead() && file.canWrite() && (file.length() % 4 == 0)) {
                result = true;
            }
        }
        return result;
    }
}
