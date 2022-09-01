package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import site.ycsb.Client;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class YcsbRunner implements Runnable {

    private static final String BINDING_CLASS = "de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb.YcsbBinding";

    private final InetSocketAddress remoteAddress;
    private final Path properties;
    private final Phase phase;
    private final int threads;
    private final boolean status;
    private final String resultFileName;
    private final String benchmarkName;
    private final int benchmarkIteration;
    private final int recordSize;

    public enum Phase {
        LOAD,
        RUN
    }

    public YcsbRunner(final InetSocketAddress remoteAddress, final Path workload, final Phase phase, final int threads, final boolean status, final String resultFileName, final String benchmarkName, final int benchmarkIteration, final int recordSize) {
        this.remoteAddress = remoteAddress;
        this.properties = workload;
        this.resultFileName = resultFileName;
        this.phase = phase;
        this.threads = threads;
        this.status = status;
        this.benchmarkName = benchmarkName;
        this.benchmarkIteration = benchmarkIteration;
        this.recordSize = recordSize;
        YcsbProperties.phase = phase;
        YcsbProperties.closeConnectionCounter = new AtomicInteger(threads);
    }

    @Override
    public void run() {
        Client.main(generateParameters(phase));
    }

    private String[] generateParameters(final Phase phase) {
        final var parameters = new ArrayList<String>();

        if (phase == Phase.LOAD) {
            parameters.add("-load");
        } else {
            parameters.add("-t");
        }

        // Set server address
        parameters.add("-p");
        parameters.add(String.format("%s=%s:%d", YcsbProperties.REMOTE_ADDRESS_PROPERTY, remoteAddress.getHostString(), remoteAddress.getPort()));

        // Set result format
        parameters.add("-p");
        parameters.add("measurementtype=hdrhistogram");
        parameters.add("-p");
        parameters.add("hdrhistogram.percentiles=50,95,99,999,9999");

        // Set exporter and output file
        if (resultFileName.isEmpty()) {
            parameters.add("-p");
            parameters.add(String.format("exporter=%s", LoggingExporter.class.getCanonicalName()));
        } else {
            CsvExporter.benchmarkName = benchmarkName;
            CsvExporter.iteration = benchmarkIteration;
            CsvExporter.connections = threads;
            CsvExporter.recordSize = recordSize;
            CsvExporter.resultFileName = resultFileName;

            parameters.add("-p");
            parameters.add(String.format("exporter=%s", CsvExporter.class.getCanonicalName()));
        }

        // Set properties file
        parameters.add("-P");
        parameters.add(properties.toAbsolutePath().toString());

        // Set binding implementation
        parameters.add("-db");
        parameters.add(BINDING_CLASS);

        // Set thread count
        parameters.add("-threads");
        parameters.add(String.valueOf(threads));

        // Enable status report
        if (status) {
            parameters.add("-s");
        }

        return parameters.toArray(new String[0]);
    }
}
