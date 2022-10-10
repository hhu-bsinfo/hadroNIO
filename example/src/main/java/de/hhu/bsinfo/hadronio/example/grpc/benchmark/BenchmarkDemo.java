package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.net.InetSocketAddress;

@CommandLine.Command(
        name = "benchmark",
        description = "Benchmark application, that tries to perform as many remote procedure calls as possible, with configurable request and answer sizes.",
        showDefaultValues = true,
        separator = " ")
public class BenchmarkDemo implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkDemo.class);
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
            names = {"-r", "--remote"},
            description = "The address to connect to.")
    private InetSocketAddress remoteAddress;

    @CommandLine.Option(
            names = {"-as", "--answer-size"},
            description = "The request return value size.",
            required = true)
    private int answerSize;

    @CommandLine.Option(
            names = {"-t", "--threshold"},
            description = "The maximum amount of RPCs to perform before flushing.")
    private int aggregationThreshold = 64;

    @CommandLine.Option(
            names = {"-b", "--blocking"},
            description = "Whether to perform a blocking (latency optimised) or non-blocking (throughput optimised) benchmark.")
    private boolean blocking = false;

    @CommandLine.Option(
            names = {"-rs", "--request-size"},
            description = "The request parameter size.")
    private int requestSize = -1;

    @CommandLine.Option(
            names = {"-m", "--messages"},
            description = "The amount of requests to perform.")
    private int requestCount = -1;

    @CommandLine.Option(
            names = {"-c", "--connections"},
            description = "The amount of client connections.")
    private int connections = 1;

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

        if (!isServer && requestCount < 0) {
            LOGGER.error("Please specify the request count");
            return;
        }

        if (!isServer && answerSize < 0) {
            LOGGER.error("Please specify the answer size");
            return;
        }

        if (bindAddress == null) {
            bindAddress = isServer ? new InetSocketAddress(DEFAULT_SERVER_PORT) : null;
        } else {
            bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        final var runnable = isServer ? new Server(bindAddress, answerSize, connections) : new Client(remoteAddress, requestCount, requestSize, answerSize, connections, aggregationThreshold, blocking, resultFileName, benchmarkName, benchmarkIteration);
        runnable.run();
    }
}
