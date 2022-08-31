package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class YcsbResult {

    private long recordSize;
    private double operationThroughput;
    private final List<Double> averageLatencies = new ArrayList<>();
    private final List<Double> minimumLatencies = new ArrayList<>();
    private final List<Double> maximumLatencies = new ArrayList<>();
    private final List<Double> percentile50thLatencies = new ArrayList<>();
    private final List<Double> percentile95thLatencies = new ArrayList<>();
    private final List<Double> percentile99thLatencies = new ArrayList<>();
    private final List<Double> percentile999thLatencies = new ArrayList<>();
    private final List<Double> percentile9999thLatencies = new ArrayList<>();

    void setRecordSize(final long recordSize) {
        this.recordSize = recordSize;
    }

    void setOperationThroughput(final double operationThroughput) {
        this.operationThroughput = operationThroughput;
    }

    void addAverageLatency(final double latency) {
        averageLatencies.add(latency);
    }

    void addMinimumLatency(final double latency) {
        minimumLatencies.add(latency);
    }

    void addMaximumLatency(final double latency) {
        maximumLatencies.add(latency);
    }

    void add50thPercentileLatency(final double latency) {
        percentile50thLatencies.add(latency);
    }

    void add95thPercentileLatency(final double latency) {
        percentile95thLatencies.add(latency);
    }

    void add99thPercentileLatency(final double latency) {
        percentile99thLatencies.add(latency);
    }

    void add999thPercentileLatency(final double latency) {
        percentile999thLatencies.add(latency);
    }

    void add9999thPercentileLatency(final double latency) {
        percentile9999thLatencies.add(latency);
    }

    void write(final String fileName, final String benchmarkName, final int iteration, final int connections) throws IOException {
        final var file = new File(fileName);
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
                .append(String.valueOf(recordSize)).append(",")
                .append(String.valueOf(operationThroughput)).append(",")
                .append(String.valueOf(calculateAverage(averageLatencies))).append(",")
                .append(String.valueOf(calculateAverage(minimumLatencies))).append(",")
                .append(String.valueOf(calculateAverage(maximumLatencies))).append(",")
                .append(String.valueOf(calculateAverage(percentile50thLatencies))).append(",")
                .append(String.valueOf(calculateAverage(percentile95thLatencies))).append(",")
                .append(String.valueOf(calculateAverage(percentile99thLatencies))).append(",")
                .append(String.valueOf(calculateAverage(percentile999thLatencies))).append(",")
                .append(String.valueOf(calculateAverage(percentile9999thLatencies))).append("\n");

        writer.flush();
        writer.close();
    }

    private double calculateAverage(final List<Double> values) {
        return values.stream().mapToDouble((value) -> value).summaryStatistics().getAverage() / 1000000;
    }
}
