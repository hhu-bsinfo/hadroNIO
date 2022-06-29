package de.hhu.bsinfo.hadronio.util;

import java.io.IOException;

public abstract class Result {

    private final long operationCount;
    private final long operationSize;
    private final long totalData;

    public Result(final long operationCount, final long operationSize) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        totalData = operationCount * operationSize;
    }

    public Result(final long operationCount, final long operationSize, final long totalData) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalData = totalData;
    }

    public long getTotalData() {
        return totalData;
    }

    public long getOperationCount() {
        return operationCount;
    }

    public long getOperationSize() {
        return operationSize;
    }

    public abstract void setMeasuredTime(final long timeInNanos);

    public abstract void writeToFile(final String fileName, final String benchmarkName, final int iteration, final int connections) throws IOException;
}
