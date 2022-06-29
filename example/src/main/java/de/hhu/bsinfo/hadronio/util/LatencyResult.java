package de.hhu.bsinfo.hadronio.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LatencyResult extends Result {

    private final LatencyStatistics latencyStatistics;

    private double totalTime;
    private double operationThroughput;

    public LatencyResult(final int operationCount, final int operationSize) {
        super(operationCount, operationSize);
        latencyStatistics = new LatencyStatistics(operationCount);
    }

    public LatencyResult(final long operationCount, final long operationSize, final long totalData, final double totalTime, final double operationThroughput, final long[] latencies) {
        super(operationCount, operationSize, totalData);
        this.totalTime = totalTime;
        this.operationThroughput = operationThroughput;
        latencyStatistics = new LatencyStatistics(latencies);
    }

    public void startSingleMeasurement() {
        latencyStatistics.start();
    }

    public void stopSingleMeasurement() {
        latencyStatistics.stop();
    }

    public void setMeasuredTime(final long timeInNanos) {
        totalTime = timeInNanos / 1000000000d;
        operationThroughput = (double) getOperationCount() / totalTime;
        latencyStatistics.sortAscending();
    }

    public double getTotalTime() {
        return totalTime;
    }

    public LatencyStatistics getStatistics() {
        return latencyStatistics;
    }

    public double getAverageLatency() {
        return latencyStatistics.getAvgNs() / 1000000000;
    }

    public double getMinimumLatency() {
        return latencyStatistics.getMinNs() / 1000000000;
    }

    public double getMaximumLatency() {
        return latencyStatistics.getMaxNs() / 1000000000;
    }

    public double getPercentileLatency(float percentile) {
        return latencyStatistics.getPercentilesNs(percentile) / 1000000000;
    }

    public double getOperationThroughput() {
        return operationThroughput;
    }

    public void writeToFile(final String fileName, final String benchmarkName, final int iteration, final int connections) throws IOException {
        final File file = new File(fileName);
        FileWriter writer;

        if (file.exists()) {
            writer = new FileWriter(fileName, true);
        } else {
            if (!file.createNewFile()) {
                throw new IOException("Unable to create file '" + fileName + "'");
            }

            writer = new FileWriter(fileName, false);
            writer.write("Benchmark,Iteration,Connections,Size,OperationThroughput,AverageLatency,MinimumLatency,MaximumLatency,50thLatency,95thLatency,99thLatency,999thLatency,9999thLatency\n");
        }

        writer.append(benchmarkName).append(",")
                .append(String.valueOf(iteration)).append(",")
                .append(String.valueOf(connections)).append(",")
                .append(String.valueOf(getOperationSize())).append(",")
                .append(String.valueOf(getOperationThroughput())).append(",")
                .append(String.valueOf(getAverageLatency())).append(",")
                .append(String.valueOf(getMinimumLatency())).append(",")
                .append(String.valueOf(getMaximumLatency())).append(",")
                .append(String.valueOf(getPercentileLatency(0.5f))).append(",")
                .append(String.valueOf(getPercentileLatency(0.95f))).append(",")
                .append(String.valueOf(getPercentileLatency(0.99f))).append(",")
                .append(String.valueOf(getPercentileLatency(0.999f))).append(",")
                .append(String.valueOf(getPercentileLatency(0.9999f))).append("\n");

        writer.flush();
        writer.close();
    }

    @Override
    public String toString() {
        return "LatencyResult {" +
                "\n\t" + ValueFormatter.formatValue("operationCount", getOperationCount()) +
                ",\n\t" + ValueFormatter.formatValue("operationSize", getOperationSize(), "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalData", getTotalData(), "Byte") +
                ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
                ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
                ",\n\t" + ValueFormatter.formatValue("averageLatency", getAverageLatency(), "s") +
                ",\n\t" + ValueFormatter.formatValue("minimumLatency", getMinimumLatency(), "s") +
                ",\n\t" + ValueFormatter.formatValue("maximumLatency", getMaximumLatency(), "s") +
                ",\n\t" + ValueFormatter.formatValue("50% Latency", getPercentileLatency(0.5f), "s") +
                ",\n\t" + ValueFormatter.formatValue("90% Latency", getPercentileLatency(0.9f), "s") +
                ",\n\t" + ValueFormatter.formatValue("95% Latency", getPercentileLatency(0.95f), "s") +
                ",\n\t" + ValueFormatter.formatValue("99% Latency", getPercentileLatency(0.99f), "s") +
                ",\n\t" + ValueFormatter.formatValue("99.9% Latency", getPercentileLatency(0.999f), "s") +
                ",\n\t" + ValueFormatter.formatValue("99.99% Latency", getPercentileLatency(0.9999f), "s") +
                "\n}";
    }
}
