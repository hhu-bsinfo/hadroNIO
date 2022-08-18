package de.hhu.bsinfo.hadronio.example.blocking.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.SyncSignal;
import de.hhu.bsinfo.hadronio.util.LatencyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

@CommandLine.Command(
        name = "latency",
        description = "Messaging latency benchmark using blocking socket channels",
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
    private InetSocketAddress bindAddress = null;

    @CommandLine.Option(
            names = {"-r", "--remote"},
            description = "The address to connect to.")
    private InetSocketAddress remoteAddress;

    @CommandLine.Option(
            names = {"-l", "--length"},
            description = "The message size.",
            required = true)
    private int messageSize;

    @CommandLine.Option(
            names = {"-m", "--messages"},
            description = "The amount of messages.",
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

    private SocketChannel socket;
    private ByteBuffer messageBuffer;
    private LatencyResult result;

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

        messageBuffer = ByteBuffer.allocateDirect(messageSize);
        result = new LatencyResult(messageCount, messageSize);

        try {
            if (isServer) {
                final var serverSocket = ServerSocketChannel.open();
                serverSocket.configureBlocking(true);
                serverSocket.bind(bindAddress);
                socket = serverSocket.accept();
                serverSocket.close();
            } else {
                socket = SocketChannel.open(remoteAddress);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to create socket channel!", e);
            return;
        }

        final var closeSignal = new SyncSignal(socket);
        final int warmupCount = (messageCount / 10) > 0 ? (messageCount / 10) : 1;

        try {
            LOGGER.info("Starting benchmark with blocking socket channels");
            socket.configureBlocking(true);

            if (isServer) {
                LOGGER.info("Starting warmup with [{}] messages", warmupCount);

                performPingPongIterationsServerWarmup(warmupCount);
                closeSignal.exchange();

                LOGGER.info("Starting benchmark with [{}] messages", messageCount);
                final long startTime = System.nanoTime();

                performPingPongIterationsServer(messageCount);
                closeSignal.exchange();

                result.setMeasuredTime(System.nanoTime() - startTime);
            } else {
                LOGGER.info("Starting warmup with [{}] messages", warmupCount);

                performPingPongIterationsClient(warmupCount);
                closeSignal.exchange();

                LOGGER.info("Starting benchmark with [{}] messages", messageCount);
                performPingPongIterationsClient(messageCount);
                closeSignal.exchange();
            }

            socket.close();
        } catch (IOException e) {
            LOGGER.error("Benchmark failed!", e);
            return;
        }

        if (isServer) {
            LOGGER.info("{}", result);
            if (!resultFileName.isEmpty()) {
                try {
                    result.writeToFile(resultFileName, benchmarkName, benchmarkIteration, 1);
                } catch (IOException e) {
                    LOGGER.error("Unable to write result to file '{}'", resultFileName, e);
                }
            }
        }
    }

    private void performPingPongIterationsServerWarmup(final int messageCount) throws IOException {
        for (int i = 0; i < messageCount; i++) {
            socket.write(messageBuffer);
            messageBuffer.flip();

            do {
                socket.read(messageBuffer);
            } while (messageBuffer.hasRemaining());

            messageBuffer.flip();
        }
    }

    private void performPingPongIterationsServer(final int messageCount) throws IOException {
        for (int i = 0; i < messageCount; i++) {
            result.startSingleMeasurement();
            socket.write(messageBuffer);
            messageBuffer.flip();

            do {
                socket.read(messageBuffer);
            } while (messageBuffer.hasRemaining());

            result.stopSingleMeasurement();
            messageBuffer.flip();
        }
    }

    private void performPingPongIterationsClient(final int messageCount) throws IOException {
        for (int i = 0; i < messageCount; i++) {
            do {
                socket.read(messageBuffer);
            } while (messageBuffer.hasRemaining());

            messageBuffer.flip();
            socket.write(messageBuffer);
            messageBuffer.flip();
        }
    }
}
