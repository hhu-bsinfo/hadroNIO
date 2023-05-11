package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import java.io.*;

public class CsvHistogramExporter extends LoggingExporter {

    static String benchmarkName;
    static int iteration;
    static int connections;
    static int recordSize;
    static String resultFileName;

    private final YcsbResult result = new YcsbResult();

    public CsvHistogramExporter(final OutputStream outputStream) {
        super(outputStream);
        result.setRecordSize(recordSize);
    }

    @Override
    public void write(final String metric, final String measurement, final int i) throws IOException {
        super.write(metric, measurement, i);
        gather(metric, measurement, i);
    }

    @Override
    public void write(final String metric, final String measurement, final long l) throws IOException {
        super.write(metric, measurement, l);
        gather(metric, measurement, l);
    }

    @Override
    public void write(final String metric, final String measurement, final double d) throws IOException {
        super.write(metric, measurement, d);
        if (YcsbProperties.phase == YcsbRunner.Phase.RUN && measurement.startsWith("Throughput")) {
            gather(metric, measurement, super.getThroughput());
        } else {
            gather(metric, measurement, d);
        }
    }

    private void gather(final String metric, final String measurement, final double value) {
        if (!metric.equals("OVERALL") && !metric.equals("READ")) {
            return;
        }

        switch (measurement) {
            case "Throughput(ops/sec)":
                result.setOperationThroughput(value);
                break;
            case "AverageLatency(us)":
                result.setAverageLatency(value / 1000000);
                break;
            case "MinLatency(us)":
                result.setMinimumLatency(value / 1000000);
                break;
            case "MaxLatency(us)":
                result.setMaximumLatency(value / 1000000);
                break;
            case "50thPercentileLatency(us)":
                result.set50thPercentileLatency(value / 1000000);
                break;
            case "95thPercentileLatency(us)":
                result.set95thPercentileLatency(value / 1000000);
                break;
            case "99thPercentileLatency(us)":
                result.set99thPercentileLatency(value / 1000000);
                break;
            case "999thPercentileLatency(us)":
                result.set999thPercentileLatency(value / 1000000);
                break;
            case "9999thPercentileLatency(us)":
                result.set9999thPercentileLatency(value / 1000000);
                break;
        }
    }

    @Override
    public void close() throws IOException {
        result.write(resultFileName, benchmarkName, iteration, connections);
        super.close();
    }
}
