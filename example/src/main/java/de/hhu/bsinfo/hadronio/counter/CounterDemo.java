package de.hhu.bsinfo.hadronio.counter;

import de.hhu.bsinfo.hadronio.HadronioProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

@CommandLine.Command(
        name = "counter",
        description = "Examples application, that sends an increasing counter and receives the other sides counter.",
        showDefaultValues = true,
        separator = " ")
public class CounterDemo implements Runnable {

    static {
        System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.HadronioProvider");
        HadronioProvider.printBanner();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterDemo.class);
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
            names = {"-b", "--blocking"},
            description = "Use blocking channels.")
    private boolean blocking = false;


    @CommandLine.Option(
        names = {"-c", "--count"},
        description = "The amount of iterations.")
    private int count = 1000;

    private boolean isRunning = true;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);

        try {
            if (blocking) {
                runBlocking();
            } else {
                runNonBlocking();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to run demo", e);
        }
    }

    private void runBlocking() throws IOException {
        final SocketChannel socket;

        if (isServer) {
            final ServerSocketChannel serverSocket = ServerSocketChannel.open();
            serverSocket.configureBlocking(true);
            serverSocket.bind(bindAddress);

            socket = serverSocket.accept();
            serverSocket.close();
        } else {
            socket = SocketChannel.open();
            socket.configureBlocking(true);
            socket.connect(remoteAddress);
        }

        final Handler handler = new Handler(null, socket, count);
        handler.runBlocking();
    }

    private void runNonBlocking() throws IOException {
        final Selector selector = Selector.open();
        ServerSocketChannel serverSocket = null;

        if (isServer) {
            serverSocket = ServerSocketChannel.open();
            serverSocket.configureBlocking(false);
            serverSocket.bind(bindAddress);

            final SelectionKey key = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            final Acceptor acceptor = new Acceptor(selector, serverSocket, count);
            key.attach(acceptor);
        } else {
            final SocketChannel socket = SocketChannel.open();
            socket.configureBlocking(false);
            socket.connect(remoteAddress);

            final SelectionKey key = socket.register(selector, SelectionKey.OP_CONNECT);
            key.attach(new Handler(key, socket, count));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Received shutdown signal");
            isRunning = false;
        }));

        while(isRunning && !selector.keys().isEmpty()) {
            selector.selectNow();

            for (final SelectionKey key : selector.selectedKeys()) {
                final Runnable runnable = (Runnable) key.attachment();
                if(runnable != null) {
                    runnable.run();
                }
            }

            selector.selectedKeys().clear();
        }

        if (serverSocket != null) {
            serverSocket.close();
        }

        selector.close();
    }
}
