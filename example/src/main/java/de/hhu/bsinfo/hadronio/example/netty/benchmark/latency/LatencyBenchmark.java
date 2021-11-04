package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.net.InetSocketAddress;

@CommandLine.Command(
    name = "latency",
    description = "Messaging latency benchmark using netty",
    showDefaultValues = true,
    separator = " ")
public class LatencyBenchmark implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LatencyBenchmark.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    @CommandLine.Option(
        names = {"-s", "--server"},
        description = "Run this instance in server mode.")
    private boolean isServer = false;

    @CommandLine.Option(
        names = {"-a", "--address"},
        description = "The address to bind to.")
    private InetSocketAddress bindAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);

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
        names = {"-c", "--count"},
        description = "The amount of messages to send/receive.",
        required = true)
    private int messageCount;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        final Runnable runnable = isServer ? new Server(bindAddress, messageSize, messageCount) : new Client(bindAddress, remoteAddress, messageSize, messageCount);
        runnable.run();
    }
}