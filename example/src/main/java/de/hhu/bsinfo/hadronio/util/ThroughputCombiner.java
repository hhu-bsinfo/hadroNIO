package de.hhu.bsinfo.hadronio.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThroughputCombiner {

    private final Collection<ThroughputResult> results = new HashSet<>();
    private final Lock lock = new ReentrantLock();
    private int operationCount;
    private int operationSize;

    public void addResult(final ThroughputResult newResult) {
        lock.lock();
        if (results.isEmpty()) {
            operationCount = newResult.getOperationCount();
            operationSize = newResult.getOperationSize();
        } else {
            for (final ThroughputResult result : results) {
                if (operationCount != result.getOperationCount() || operationSize != result.getOperationSize()) {
                    lock.unlock();
                    throw new IllegalArgumentException("Incompatible result!");
                }
            }
        }

        results.add(newResult);
        lock.unlock();
    }

    public ThroughputResult getCombinedResult() {
        double operationThroughput = 0;
        double dataThroughput = 0;
        double totalTime = 0;
        long totalData = 0;

        for (final ThroughputResult result : results) {
            operationThroughput += result.getOperationThroughput();
            dataThroughput += result.getDataThroughput();
            totalData += result.getTotalData();

            if (result.getTotalTime() > totalTime) {
                totalTime = result.getTotalTime();
            }
        }

        return new ThroughputResult(operationCount, operationSize, totalData, totalTime, operationThroughput, dataThroughput);
    }
}
