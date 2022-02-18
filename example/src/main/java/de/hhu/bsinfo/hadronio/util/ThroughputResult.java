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
        totalData = (long) operationCount * (long) operationSize;
    }

    ThroughputResult(int operationCount, int operationSize, double totalTime, double operationThroughput, double dataThroughput) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalTime = totalTime;
        this.operationThroughput = operationThroughput;
        this.dataThroughput = dataThroughput;
        totalData = (long) operationCount * (long) operationSize;
    }

    public int getOperationCount() {
        return operationCount;
    }

    public int getOperationSize() {
        return operationSize;
    }

    public double getOperationThroughput() {
        return operationThroughput;
    }

    public double getDataThroughput() {
        return dataThroughput;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public void setMeasuredTime(final long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;

        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    public void setOperationThroughput(double operationThroughput) {
        this.operationThroughput = operationThroughput;
    }

    public void setDataThroughput(double dataThroughput) {
        this.dataThroughput = dataThroughput;
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
