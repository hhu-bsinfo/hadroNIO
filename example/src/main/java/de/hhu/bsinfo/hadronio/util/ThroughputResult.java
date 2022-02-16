package de.hhu.bsinfo.hadronio.util;

public class ThroughputResult {

    private final int operationCount;
    private final int operationSize;
    private final long totalData;

    private double totalTime;
    private double operationThroughput;
    private double dataThroughput;

    public ThroughputResult(final int operationCount, final int operationSize) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalData = (long) operationCount * (long) operationSize;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public double getOperationThroughput() {
        return operationThroughput;
    }

    public double getDataThroughput() {
        return dataThroughput;
    }

    public void setMeasuredTime(final long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;

        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    @Override
    public String toString() {
        return "ThroughputResult {" +
                "\n\t" + ValueFormatter.formatValue("operationCount", operationCount) +
                ",\n\t" + ValueFormatter.formatValue("operationSize", operationSize, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalData", totalData, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
                ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
                ",\n\t" + ValueFormatter.formatValue("dataThroughput", dataThroughput, "Byte/s") +
                "\n}";
    }
}
