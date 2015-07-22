package org.sw.util;

import org.sw.util.array.IntByteUtil;
import org.sw.util.file.FileSortingThread;
import org.sw.util.params.FileChecker;
import org.sw.util.params.ThreadCountChecker;
import org.sw.util.timer.StatisticsTimer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class BinaryFileSortUtilMain {

    public static void main(String[] args) {
        String filename = getParam("-f", args);
        String threadCount = getParam("-t", args);
        if (isParamPresent("-ds",args)) {
            FileSortingThread.DIRECT_SORT_BUFFER = true;
        }
        if (isParamPresent("-dm",args)) {
            FileSortingThread.DIRECT_MERGE_BUFFER = true;
        }
        if (FileChecker.isValid(filename) && ThreadCountChecker.isValid(threadCount)) {
            StatisticsTimer timer = new StatisticsTimer().start();
            File file = new File(filename);
            int nThreads = Integer.parseInt(threadCount);

            AtomicInteger nThreadsAvailable = new AtomicInteger(nThreads);
            System.out.println("Start mergesort Filesize=" + (file.length() >> 10) + "kB" +
                    " Threads=" + threadCount +
                    " CPUs=" + Runtime.getRuntime().availableProcessors() +
                    " MEM_SORT_SIZE=" + (FileSortingThread.SORTBUFF_INT_SIZE >> 8) + "kB" +
                    " BUFF_WRITE_SIZE=" + (FileSortingThread.MERGEBUFF_INT_SIZE >> 8) + "kB" +
                    " DIRECTSORT=" + FileSortingThread.DIRECT_SORT_BUFFER +
                    " DIRECTMERGE=" + FileSortingThread.DIRECT_MERGE_BUFFER);
            FileSortingThread fst = new FileSortingThread(nThreadsAvailable, filename, 0, IntByteUtil.intIdx(file.length()) - 1, IntByteUtil.getMax2Power(nThreads), 0);

            try {
                fst.mergeSort();
                timer.stop().print("File sorting is completed!");
            } catch (IOException e) {
                System.out.println("File sorting failed: ");
                e.printStackTrace();
            }
        } else {
            printUsage();
        }
    }

    private static String getParam(String param, String[] args) {
        String result = null;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(param)) {
                result = args[i + 1];
                break;
            }
        }
        return result;
    }

    private static boolean isParamPresent(String param, String[] args) {
        boolean result = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(param)) {
                result = true;
                break;
            }
        }
        return result;
    }

    private static void printUsage() {
        System.out.println("Binary file sorting utility v1.0");
        System.out.println("<sorting util> -f <file_to_sort> -t <threads_count> -ds");
    }
}
