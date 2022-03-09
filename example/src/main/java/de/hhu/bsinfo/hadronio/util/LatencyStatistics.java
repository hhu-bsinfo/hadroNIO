package de.hhu.bsinfo.hadronio.util;

import java.util.Arrays;

/**
 * Statistics class to measure latency.
 *
 * @author Stefan Nothaas, HHU
 */
class LatencyStatistics {
    private final long[] times;
    private int pos;

    private long tmpTime;

    /**
     * Constructor
     *
     * @param size Total number of measurements to record.
     */
    LatencyStatistics(int size) {
        times = new long[size];
        pos = 0;
    }

    LatencyStatistics(long[] times) {
        this.times = times;
        pos = times.length;
        sortAscending();
    }

    /**
     * Start measuring time.
     */
    void start() {
        tmpTime = System.nanoTime();
    }

    /**
     * Stop measuring time (must be preceded by a call to start).
     */
    void stop() {
        times[pos++] = System.nanoTime() - tmpTime;
    }

    /**
     * Sort all currently available measurements in ascending order.
     */
    void sortAscending() {
        Arrays.sort(times);
    }

    /**
     * Get the lowest time in ns.
     *
     * @return Time in ns.
     */
    double getMinNs() {
        return times[0];
    }

    /**
     * Get the highest time in ns.
     *
     * @return Time in ns.
     */
    double getMaxNs() {
        return times[pos - 1];
    }

    /**
     * Get the total time in ns.
     *
     * @return Time in ns.
     */
    double getTotalNs() {
        double tmp = 0;

        for (int i = 0; i < pos; i++) {
            tmp += times[i];
        }

        return tmp;
    }

    /**
     * Get the average time in ns.
     *
     * @return Time in ns.
     */
    double getAvgNs() {
        return getTotalNs() / pos;
    }

    /**
     * Get the Xth percentiles value (Stats object must be sorted).
     *
     * @return Value in ns.
     */
    double getPercentilesNs(float perc) {
        if (perc < 0.0 || perc > 1.0) {
            throw new IllegalArgumentException("Percentage must be between 0 and 1");
        }

        return times[(int) Math.ceil(perc * pos) - 1];
    }

    long[] getTimes() {
        return times;
    }
}
