package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import java.io.*;

public class CsvExporter extends LoggingExporter {

    static String benchmarkName;
    static int iteration;
    static int connections;
    static int recordSize;
    static String resultFileName;

    private final YcsbResult result = new YcsbResult();

    public CsvExporter(final OutputStream outputStream) {
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
        gather(metric, measurement, d);
    }

    private void gather(final String metric, final String measurement, final double value) {
        if (metric.equals("CLEANUP")) {
            return;
        }

        switch (measurement) {
            case "Throughput(ops/sec)":
                result.setOperationThroughput(value);
                break;
            case "AverageLatency(us)":
                result.addAverageLatency(value);
                break;
            case "MinLatency(us)":
                result.addMinimumLatency(value);
                break;
            case "MaxLatency(us)":
                result.addMaximumLatency(value);
                break;
            case "50thPercentileLatency(us)":
                result.add50thPercentileLatency(value);
                break;
            case "95thPercentileLatency(us)":
                result.add95thPercentileLatency(value);
                break;
            case "99thPercentileLatency(us)":
                result.add99thPercentileLatency(value);
                break;
            case "999thPercentileLatency(us)":
                result.add999thPercentileLatency(value);
                break;
            case "9999thPercentileLatency(us)":
                result.add9999thPercentileLatency(value);
                break;
        }
    }

    @Override
    public void close() throws IOException {
        result.write(resultFileName, benchmarkName, iteration, connections);
        super.close();
    }
}
