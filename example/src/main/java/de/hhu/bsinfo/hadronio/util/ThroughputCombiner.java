package de.hhu.bsinfo.hadronio.util;

import java.util.Collection;
import java.util.HashSet;

public class ThroughputCombiner {

    private final Collection<ThroughputResult> results = new HashSet<>();
    private int operationCount;
    private int operationSize;

    public void addResult(final ThroughputResult newResult) {
        if (results.isEmpty()) {
            operationCount = newResult.getOperationCount();
            operationSize = newResult.getOperationSize();
        } else {
            for (final ThroughputResult result : results) {
                if (operationCount != result.getOperationCount() || operationSize != result.getOperationSize()) {
                    throw new IllegalArgumentException("Incompatible result!");
                }
            }
        }

        results.add(newResult);
    }

    public ThroughputResult getCombinedResult() {
        double operationThroughput = 0;
        double dataThroughput = 0;
        double totalTime = 0;
        for (final ThroughputResult result : results) {
            operationThroughput += result.getOperationThroughput();
            dataThroughput += result.getDataThroughput();
            if (result.getTotalTime() > totalTime) {
                totalTime = result.getTotalTime();
            }
        }

        return new ThroughputResult(operationCount, operationSize, totalTime, operationThroughput, dataThroughput);
    }
}
