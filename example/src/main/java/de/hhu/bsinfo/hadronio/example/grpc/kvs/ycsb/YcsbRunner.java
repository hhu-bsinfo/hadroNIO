package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import site.ycsb.Client;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;

public class YcsbRunner implements Runnable {

    private static final String BINDING_CLASS = "de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb.YcsbBinding";
    private static final String JSON_EXPORTER = "site.ycsb.measurements.exporter.JSONMeasurementsExporter";

    private final InetSocketAddress remoteAddress;
    private final Path properties;
    private final Path export;
    private final Phase phase;
    private final int threads;
    private final boolean status;

    public enum Phase {
        LOAD,
        RUN
    }

    public YcsbRunner(InetSocketAddress remoteAddress, Path workload, Path export, Phase phase, int threads, boolean status) {
        this.remoteAddress = remoteAddress;
        this.properties = workload;
        this.export = export;
        this.phase = phase;
        this.threads = threads;
        this.status = status;
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

        // Write results to file if path was set
        if (export != null) {
            parameters.add("-p");
            parameters.add(String.format("exportfile=%s", export.toAbsolutePath()));
            parameters.add("-p");
            parameters.add(String.format("exporter=%s", JSON_EXPORTER));
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
