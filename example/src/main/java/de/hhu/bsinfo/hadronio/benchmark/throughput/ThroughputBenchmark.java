package de.hhu.bsinfo.hadronio.benchmark.throughput;

import de.hhu.bsinfo.hadronio.UcxProvider;
import de.hhu.bsinfo.hadronio.util.Result;
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
import java.nio.charset.StandardCharsets;

@CommandLine.Command(
        name = "throughput",
        description = "Messaging throughput benchmark.",
        showDefaultValues = true,
        separator = " ")
public class ThroughputBenchmark implements Runnable {

    static {
        System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.UcxProvider");
        UcxProvider.printBanner();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputBenchmark.class);
    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final String CLOSE_SIGNAL = "CLOSE";

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
            names = {"-b", "--blocking"},
            description = "Use blocking channels.")
    private boolean blocking = false;

    @CommandLine.Option(
            names = {"-l", "--length"},
            description = "The message size.")
    private int messageSize = 1024;

    @CommandLine.Option(
            names = {"-c", "--count"},
            description = "The amount of messages.")
    private int messageCount = 1000;

    private SocketChannel socket;
    private ByteBuffer messageBuffer;
    private ByteBuffer closeBuffer;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);
        messageBuffer = ByteBuffer.allocateDirect(messageSize);
        closeBuffer = ByteBuffer.allocateDirect(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL).capacity());
        final Result result = new Result(messageCount, messageSize);

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
            LOGGER.error("Failed to create socket channel!", e);
            return;
        }

        final long time;
        try {
            if (blocking) {
                time = runBlocking();
            } else {
                time = runNonBlocking();
            }

            socket.close();
        } catch (IOException e) {
            LOGGER.error("Benchmark failed!", e);
            return;
        }

        result.setMeasuredTime(time);
        LOGGER.info("Benchmark result:\n{}", result);
    }

    private long runBlocking() throws IOException {
        LOGGER.info("Starting benchmark with blocking socket channels");
        socket.configureBlocking(true);
        final long startTime = System.nanoTime();
        final long measuredTime;

        if (isServer) {
            for (int i = 0; i < messageCount; i++) {
                LOGGER.trace("Sending [{}]", i);
                socket.write(messageBuffer);
                messageBuffer.clear();
                LOGGER.trace("Sent [{}]", i);
            }

            LOGGER.info("Waiting for close signal");
            socket.write(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL));
            do {
                socket.read(closeBuffer);
            } while (closeBuffer.hasRemaining());
        } else {
            for (int i = 0; i < messageCount; i++) {
                LOGGER.trace("Receiving [{}]", i);
                do {
                    socket.read(messageBuffer);
                } while (messageBuffer.hasRemaining());

                messageBuffer.clear();
                LOGGER.trace("Received [{}]", i);
            }

            LOGGER.info("Waiting for close signal");
            do {
                socket.read(closeBuffer);
            } while (closeBuffer.hasRemaining());
            socket.write(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL));
        }

        measuredTime = System.nanoTime() - startTime;
        LOGGER.info("Received close signal! Measure time: [{}]", measuredTime);

        closeBuffer.flip();
        final String receivedCloseSignal = StandardCharsets.UTF_8.decode(closeBuffer).toString();
        if (!receivedCloseSignal.equals(CLOSE_SIGNAL)) {
            throw new IOException("Got wrong close signal! Expected: [" + CLOSE_SIGNAL + "], Got: [" + receivedCloseSignal + "]");
        }

        return measuredTime;
    }

    private long runNonBlocking() throws IOException {
        LOGGER.info("Starting benchmark with non-blocking socket channels");
        socket.configureBlocking(false);

        final Selector selector = Selector.open();
        if (isServer) {
            final SelectionKey key = socket.register(selector, SelectionKey.OP_WRITE);
            key.attach(new ServerHandler(socket, key, messageBuffer, messageCount));
        } else {
            final SelectionKey key = socket.register(selector, SelectionKey.OP_READ);
            key.attach(new ClientHandler(socket, key, messageBuffer, messageCount));
        }

        final long startTime = System.nanoTime();
        final long measuredTime;

        while (!selector.keys().isEmpty()) {
            selector.selectNow();

            for (final SelectionKey key : selector.selectedKeys()) {
                ((Runnable) key.attachment()).run();
            }

            selector.selectedKeys().clear();
        }

        socket.configureBlocking(true);
        LOGGER.info("Waiting for close signal");
        if (isServer) {
            socket.write(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL));
            do {
                socket.read(closeBuffer);
            } while (closeBuffer.hasRemaining());
        } else {
            do {
                socket.read(closeBuffer);
            } while (closeBuffer.hasRemaining());
            socket.write(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL));
        }

        measuredTime = System.nanoTime() - startTime;
        LOGGER.info("Received close signal! Measured time: [{}]", measuredTime);

        closeBuffer.flip();
        final String receivedCloseSignal = StandardCharsets.UTF_8.decode(closeBuffer).toString();
        if (!receivedCloseSignal.equals(CLOSE_SIGNAL)) {
            throw new IOException("Got wrong close signal! Expected: [" + CLOSE_SIGNAL + "], Got: [" + receivedCloseSignal + "]");
        }

        return measuredTime;
    }
}
