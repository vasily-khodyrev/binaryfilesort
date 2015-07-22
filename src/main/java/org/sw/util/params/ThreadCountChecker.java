package org.sw.util.params;

public class ThreadCountChecker {
    public static boolean isValid(String threadcount) {
        boolean result = false;
        int thread_cnt = 0;
        if (threadcount != null) {
            try {
                thread_cnt = Integer.parseInt(threadcount);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            if (thread_cnt > 0) {
                result = true;
            }
        }
        return result;
    }
}
