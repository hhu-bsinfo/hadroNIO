package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.io.OutputStream;

public class LoggingExporter implements MeasurementsExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingExporter.class.getCanonicalName().replace(LoggingExporter.class.getSimpleName(), "YCSB"));

    private static final int METRIC_LENGTH = 35;
    private static final int MEASUREMENT_LENGTH = 23;
    private static final int VALUE_LENGTH = 15;

    private long runTime;
    private double throughput;

    public LoggingExporter(final OutputStream outputStream) {
        write("Metric", "Measurement", "Value");
        LOGGER.info("-".repeat(METRIC_LENGTH + 1) + "|" + "-".repeat(MEASUREMENT_LENGTH + 2) + "|" + "-".repeat(VALUE_LENGTH + 1));
    }

    @Override
    public void write(final String metric, final String measurement, final int i) throws IOException {
        final var value = formatUnit(measurement, String.valueOf(i));
        write(metric, measurement, value);
    }

    @Override
    public void write(final String metric, final String measurement, final long l) throws IOException {
        if (measurement.startsWith("RunTime")) {
            runTime = l - YcsbBinding.getInitTime();
            write(metric, "InitTime(ms)", YcsbBinding.getInitTime());
        }

        final var value = formatUnit(measurement, String.valueOf(l));
        write(metric, measurement, value);
    }

    @Override
    public void write(final String metric, final String measurement, final double d) throws IOException {
        if (YcsbProperties.phase == YcsbRunner.Phase.RUN && measurement.startsWith("Throughput")) {
            throughput = YcsbBinding.getOperationCount() / (runTime / 1000.0);
            final var value = formatUnit(measurement, String.format("%.03f", throughput));
            write(metric, measurement, value);
        } else {
            final var value = formatUnit(measurement, String.format("%.03f", d));
            write(metric, measurement, value);
        }
    }

    @Override
    public void close() throws IOException {}

    public double getThroughput() {
        return throughput;
    }

    private static String formatUnit(final String measurement, final String value) {
        if (measurement.contains("(")) {
            var unit = measurement.substring(measurement.indexOf('(') + 1, measurement.indexOf(')'));
            var retValue = unit.equals("%") ? value : value + " ";

            return retValue + unit;
        }

        return value;
    }

    private static void write(final String metric, final String measurement, final String value) {
        final var metricString = String.format("%-" + METRIC_LENGTH + "s", metric);
        final var measurementString = String.format("%-" + MEASUREMENT_LENGTH + "s", (measurement.contains("(") ? measurement.substring(0, measurement.indexOf('(')) : measurement));
        LOGGER.info("{} | {} | {}", metricString, measurementString, value);
    }
}
