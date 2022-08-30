package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.io.OutputStream;

public class LoggingExporter implements MeasurementsExporter {

    private static final int METRING_LENGTH = 35;
    private static final int MEASUREMENT_LENGTH = 21;
    private static final int VALUE_LENGTH = 15;

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingExporter.class.getCanonicalName().replace(LoggingExporter.class.getSimpleName(), "YCSB"));

    public LoggingExporter(final OutputStream outputStream) {
        logResult("Metric", "Measurement", "Value");
        LOGGER.info("-".repeat(METRING_LENGTH + 1) + "|" + "-".repeat(MEASUREMENT_LENGTH + 2) + "|" + "-".repeat(VALUE_LENGTH + 1));
    }

    @Override
    public void write(final String metric, final String measurement, final int i) throws IOException {
        final var value = formatUnit(measurement, String.valueOf(i));
        logResult(metric, measurement, value);
    }

    @Override
    public void write(final String metric, final String measurement, final long l) throws IOException {
        final var value = formatUnit(measurement, String.valueOf(l));
        logResult(metric, measurement, value);
    }

    @Override
    public void write(final String metric, final String measurement, final double d) throws IOException {
        final var value = formatUnit(measurement, String.format("%.03f", d));
        logResult(metric, measurement, value);
    }

    @Override
    public void close() throws IOException {}

    private static String formatUnit(final String measurement, final String value) {
        if (measurement.contains("(")) {
            var unit = measurement.substring(measurement.indexOf('(') + 1, measurement.indexOf(')'));
            var retValue = unit.equals("%") ? value : value + " ";

            return retValue + unit;
        }

        return value;
    }

    private static void logResult(final String metric, final String measurement, final String value) {
        final var metricString = String.format("%-" + METRING_LENGTH + "s", metric);
        final var measurementString = String.format("%-" + MEASUREMENT_LENGTH + "s", (measurement.contains("(") ? measurement.substring(0, measurement.indexOf('(')) : measurement));
        LOGGER.info("{} | {} | {}", metricString, measurementString, value);
    }
}
