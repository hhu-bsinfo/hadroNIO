package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class YcsbResult {

    private long recordSize;
    private double operationThroughput;
    private double averagelatency;
    private double minimumlatency;
    private double maximumlatency;
    private double percentile50thlatency;
    private double percentile95thlatency;
    private double percentile99thlatency;
    private double percentile999thlatency;
    private double percentile9999thlatency;

    void setRecordSize(final long recordSize) {
        this.recordSize = recordSize;
    }

    void setOperationThroughput(final double operationThroughput) {
        this.operationThroughput = operationThroughput;
    }

    void setAverageLatency(final double latency) {
        averagelatency = latency;
    }

    void setMinimumLatency(final double latency) {
        minimumlatency = latency;
    }

    void setMaximumLatency(final double latency) {
        maximumlatency = latency;
    }

    void set50thPercentileLatency(final double latency) {
        percentile50thlatency = latency;
    }

    void set95thPercentileLatency(final double latency) {
        percentile95thlatency = latency;
    }

    void set99thPercentileLatency(final double latency) {
        percentile99thlatency = latency;
    }

    void set999thPercentileLatency(final double latency) {
        percentile999thlatency = latency;
    }

    void set9999thPercentileLatency(final double latency) {
        percentile9999thlatency = latency;
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
                .append(String.valueOf(averagelatency)).append(",")
                .append(String.valueOf(minimumlatency)).append(",")
                .append(String.valueOf(maximumlatency)).append(",")
                .append(String.valueOf(percentile50thlatency)).append(",")
                .append(String.valueOf(percentile95thlatency)).append(",")
                .append(String.valueOf(percentile99thlatency)).append(",")
                .append(String.valueOf(percentile999thlatency)).append(",")
                .append(String.valueOf(percentile9999thlatency)).append("\n");

        writer.flush();
        writer.close();
    }
}
