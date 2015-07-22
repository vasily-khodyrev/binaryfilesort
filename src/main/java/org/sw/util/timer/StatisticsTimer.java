package org.sw.util.timer;

public class StatisticsTimer {

    private int count;
    private long total;
    private long time;
    private long min = Long.MAX_VALUE;
    private long max =Long.MIN_VALUE;

    public StatisticsTimer start() {
        time = System.currentTimeMillis();
        count++;
        return this;
    }

    public long getTime() {
        return time;
    }

    public void print(String message) {
        System.out.printf("Execution time = %s min %.2f sec",
                time / 1000 / 60,
                (time % (60 * 1000)) / 1000.0);
        System.out.println(" - " + message);
    }

    public void printStat(String message) {
        System.out.printf("Execution time Run=%s Min=%s min %.2f sec Max=%s min %.2f sec Avg=%s min %.2f sec",
                count,
                min / 1000 / 60,
                (min % (60 * 1000)) / 1000.0,
                max / 1000 / 60,
                (max % (60 * 1000)) / 1000.0,
                total /count / 1000 / 60,
                ((total/count) % (60 * 1000)) / 1000.0);
        System.out.println(" - " + message);
    }

    public StatisticsTimer stop() {
        time = System.currentTimeMillis() - time;
        total += time;
        min = Math.min(min,time);
        max=Math.max(max,time);

        return this;
    }

}
