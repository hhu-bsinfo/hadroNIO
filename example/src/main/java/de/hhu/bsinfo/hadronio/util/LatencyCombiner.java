package de.hhu.bsinfo.hadronio.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LatencyCombiner implements Combiner {

    private final Collection<LatencyResult> results = new HashSet<>();
    private final Lock lock = new ReentrantLock();
    private long operationCount;
    private long operationSize;

    @Override
    public void addResult(final Result newResult) {
        if (!(newResult instanceof LatencyResult)) {
            throw new IllegalArgumentException("Latency combiner can only combine latency results!");
        }

        lock.lock();
        if (results.isEmpty()) {
            operationCount = newResult.getOperationCount();
            operationSize = newResult.getOperationSize();
        } else {
            for (final LatencyResult result : results) {
                if (operationCount != result.getOperationCount() || operationSize != result.getOperationSize()) {
                    lock.unlock();
                    throw new IllegalArgumentException("Incompatible result!");
                }
            }
        }

        results.add((LatencyResult) newResult);
        lock.unlock();
    }

    @Override
    public LatencyResult getCombinedResult() {
        double operationThroughput = 0;
        double totalTime = 0;
        long totalData = 0;
        long operationCount = 0;
        final ArrayList<Long> latencySet = new ArrayList<>();

        for (final LatencyResult result : results) {
            operationThroughput += result.getOperationThroughput();
            totalData += result.getTotalData();
            operationCount += result.getOperationCount();

            if (result.getTotalTime() > totalTime) {
                totalTime = result.getTotalTime();
            }

            for (final long latency : result.getStatistics().getTimes()) {
                latencySet.add(latency);
            }
        }

        return new LatencyResult(operationCount, operationSize, totalData, totalTime, operationThroughput, latencySet.stream().mapToLong(Long::longValue).toArray());
    }
}
