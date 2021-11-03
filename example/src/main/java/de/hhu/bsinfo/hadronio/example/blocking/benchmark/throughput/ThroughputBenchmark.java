package de.hhu.bsinfo.hadronio.example.blocking.benchmark.throughput;

import de.hhu.bsinfo.hadronio.HadronioProvider;
import de.hhu.bsinfo.hadronio.util.CloseSignal;
import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
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
    private InetSocketAddress bindAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);

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
            names = {"-c", "--count"},
            description = "The amount of messages.",
            required = true)
    private int messageCount;

    private SocketChannel socket;
    private ByteBuffer messageBuffer;
    private ThroughputResult result;
    private CloseSignal closeSignal;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);
        messageBuffer = ByteBuffer.allocateDirect(messageSize);
        result = new ThroughputResult(messageCount, messageSize);

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

        closeSignal = new CloseSignal(socket);

        try {
            LOGGER.info("Starting benchmark with blocking socket channels");
            socket.configureBlocking(true);
            final long startTime = System.nanoTime();

            if (isServer) {
                for (int i = 1; i <= messageCount; i++) {
                    socket.write(messageBuffer);
                    if (messageBuffer.hasRemaining()) {
                        LOGGER.error("Buffer not fully written!");
                    }

                    messageBuffer.clear();
                }
            } else {
                for (int i = 1; i <= messageCount; i++) {
                    do {
                        socket.read(messageBuffer);
                    } while (messageBuffer.hasRemaining());

                    messageBuffer.clear();
                }
            }

            closeSignal.exchange();
            result.setMeasuredTime(System.nanoTime() - startTime);

            socket.close();
        } catch (IOException e) {
            LOGGER.error("Benchmark failed!", e);
            return;
        }

        if (isServer) {
            LOGGER.info("Benchmark result:\n{}", result);
        }
    }
}
