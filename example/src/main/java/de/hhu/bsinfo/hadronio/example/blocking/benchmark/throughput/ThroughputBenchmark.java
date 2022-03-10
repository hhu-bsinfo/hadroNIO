package de.hhu.bsinfo.hadronio.example.blocking.benchmark.throughput;

import de.hhu.bsinfo.hadronio.util.SyncSignal;
import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

@CommandLine.Command(
        name = "throughput",
        description = "Messaging throughput benchmark using blocking socket channels",
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

    private ByteBuffer[] sendBuffers;
    private ByteBuffer receiveBuffer;
    private SocketChannel socket;

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

        receiveBuffer = ByteBuffer.allocateDirect(messageSize);
        sendBuffers = new ByteBuffer[aggregationThreshold];
        for (int i = 0; i < aggregationThreshold; i++) {
            sendBuffers[i] = ByteBuffer.allocateDirect(messageSize);
        }

        try {
            if (isServer) {
                final ServerSocketChannel serverSocket = ServerSocketChannel.open();
                serverSocket.configureBlocking(true);
                serverSocket.bind(bindAddress);
                socket = serverSocket.accept();
                serverSocket.close();
            } else {
                socket = SocketChannel.open(remoteAddress);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to setup connection!", e);
            return;
        }

        final ThroughputResult result = new ThroughputResult(messageCount, messageSize);
        final SyncSignal closeSignal = new SyncSignal(socket);
        final int warmupCount = (messageCount / 10) > 0 ? (messageCount / 10) : 1;

        try {
            LOGGER.info("Starting benchmark with blocking socket channels");
            socket.configureBlocking(true);

            if (isServer) {
                LOGGER.info("Starting warmup with [{}] messages", warmupCount);

                sendMessages(warmupCount);
                closeSignal.exchange();

                LOGGER.info("Starting benchmark with [{}] messages", messageCount);
                final long startTime = System.nanoTime();

                sendMessages(messageCount);
                closeSignal.exchange();

                result.setMeasuredTime(System.nanoTime() - startTime);
            } else {
                LOGGER.info("Starting warmup with [{}] messages", warmupCount);

                receiveMessages(warmupCount);
                closeSignal.exchange();

                LOGGER.info("Starting benchmark with [{}] messages", messageCount);
                receiveMessages(messageCount);
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
                    result.writeToFile(resultFileName, benchmarkName, benchmarkIteration);
                } catch (IOException e) {
                    LOGGER.error("Unable to write result to file '{}'", resultFileName, e);
                }
            }
        }
    }

    private void sendMessages(final int messageCount) throws IOException {
        for (int i = 0; i < messageCount; i += aggregationThreshold) {
            socket.write(sendBuffers, 0, aggregationThreshold < messageCount - i ? aggregationThreshold : messageCount - i);

            for (int j = 0; j < aggregationThreshold; j++) {
                sendBuffers[j].clear();
            }
        }
    }

    private void receiveMessages(final int messageCount) throws IOException {
        for (int i = 1; i <= messageCount; i++) {
            do {
                socket.read(receiveBuffer);
            } while (receiveBuffer.hasRemaining());

            receiveBuffer.clear();
        }
    }
}
