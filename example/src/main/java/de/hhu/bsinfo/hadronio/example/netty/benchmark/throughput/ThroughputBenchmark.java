package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "throughput",
    description = "Messaging throughput benchmark using netty",
    showDefaultValues = true,
    separator = " ")
public class ThroughputBenchmark implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputBenchmark.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    @CommandLine.Option(
        names = {"-s", "--server"},
        description = "Run this instance in server mode.")
    private boolean isServer = false;

    @CommandLine.Option(
        names = {"-a", "--address"},
        description = "The address to bind to.")
    private InetSocketAddress bindAddress = null;

    @CommandLine.Option(
        names = {"-t", "--threshold"},
        description = "The maximum amount of buffers to aggregate.")
    private int aggregationThreshold = 64;

    @CommandLine.Option(
            names = {"-c", "--connections"},
            description = "The amount of connections to use for sending/receiving.")
    private int connections = 1;

    @CommandLine.Option(
        names = {"-r", "--remote"},
        description = "The address to connect to.")
    private InetSocketAddress remoteAddress;

    @CommandLine.Option(
        names = {"-l", "--length"},
        description = "The messages size.",
        required = true)
    private int messageSize;

    @CommandLine.Option(
        names = {"-m", "--messages"},
        description = "The amount of messages to send/receive.",
        required = true)
    private int messageCount;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Path to the result file, to which the CSV data shall be written.")
    private String resultFileName = "";

    @CommandLine.Option(
            names = {"-n", "--name"},
            description = "Benchmark name to use, when writing the result to a file.")
    private String benchmarkName = "";

    @CommandLine.Option(
            names = {"-i", "--iteration"},
            description = "Iteration number to use, when writing the result to a file.")
    private int benchmarkIteration = 0;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        if (bindAddress == null) {
            bindAddress = isServer ? new InetSocketAddress(DEFAULT_SERVER_PORT) : null;
        } else {
            bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        final var runnable = isServer ? new Server(bindAddress, messageSize, messageCount, aggregationThreshold, connections, resultFileName, benchmarkName, benchmarkIteration) :
                new Client(bindAddress, remoteAddress, messageSize, messageCount, connections);
        runnable.run();
    }
}