package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

public class CsvTimeSeriesExporter implements MeasurementsExporter {

    final FileWriter writer;

    public CsvTimeSeriesExporter(final OutputStream outputStream) {
        final var fileName = CsvHistogramExporter.resultFileName == null ? "result.csv" : CsvHistogramExporter.resultFileName;
        final var file = new File(fileName);

        try {
            if (file.exists()) {
                writer = new FileWriter(fileName, true);
            } else {
                if (!file.createNewFile()) {
                    throw new IOException("Unable to create file '" + fileName + "'");
                }

                writer = new FileWriter(fileName, false);
            }

            writer.append("Metric,Count,Latency\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final String metric, final String measurement, final int i) throws IOException {
        write(metric, measurement, String.valueOf(i));
    }

    @Override
    public void write(final String metric, final String measurement, final long l) throws IOException {
        write(metric, measurement, String.valueOf(l));
    }

    @Override
    public void write(final String metric, final String measurement, final double d) throws IOException {
        write(metric, measurement, String.valueOf(d));
    }

    private void write(final String metric, final String measurement, final String value) throws IOException {
        if (!metric.equals("READ") && !metric.equals("UPDATE") && !metric.equals("INSERT")) {
            return;
        }

        writer.append(metric).append(",")
                .append(measurement).append(",")
                .append(value).append("\n");
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
