package de.hhu.bsinfo.hadronio.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public class LatencyCombiner {

    private final Collection<LatencyResult> results = new HashSet<>();
    private int operationCount;
    private int operationSize;

    public void addResult(final LatencyResult newResult) {
        if (results.isEmpty()) {
            operationCount = newResult.getOperationCount();
            operationSize = newResult.getOperationSize();
        } else {
            for (final LatencyResult result : results) {
                if (operationCount != result.getOperationCount() || operationSize != result.getOperationSize()) {
                    throw new IllegalArgumentException("Incompatible result!");
                }
            }
        }

        results.add(newResult);
    }

    public LatencyResult getCombinedResult() {
        final ArrayList<Long> latencySet = new ArrayList<>();
        for (final LatencyResult result : results) {
            for (final long latency : result.getStatistics().getTimes()) {
                latencySet.add(latency);
            }
        }

        return new LatencyResult(operationCount, operationSize, latencySet.stream().mapToLong(Long::longValue).toArray());
    }
}
