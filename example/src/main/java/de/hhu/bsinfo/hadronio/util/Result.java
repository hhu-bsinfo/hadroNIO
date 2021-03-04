package de.hhu.bsinfo.hadronio.util;

public class Result {

    private final int operationCount;
    private final int operationSize;
    private final long totalData;

    private double totalTime;
    private double operationThroughput;
    private double dataThroughput;

    public Result(int operationCount, int operationSize) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalData = (long) operationCount * (long) operationSize;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public void setMeasuredTime(long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;

        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    @Override
    public String toString() {
        return "ThroughputMeasurement {" +
                "\n\t" + ValueFormatter.formatValue("operationCount", operationCount) +
                ",\n\t" + ValueFormatter.formatValue("operationSize", operationSize, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalData", totalData, "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
                ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
                ",\n\t" + ValueFormatter.formatValue("dataThroughput", dataThroughput, "Byte/s") +
                "\n}";
    }
}
