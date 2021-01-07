package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.InetSocketAddressConverter;
import de.hhu.bsinfo.hadronio.util.ResourceHandler;
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
        name = "example",
        description = "",
        showDefaultValues = true,
        separator = " ")
public class Application implements Runnable {

    static {
        System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.UcxProvider");
        UcxProvider.printBanner();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    @CommandLine.Option(
            names = {"-s", "--server"},
            description = "Runs this instance in server mode.")
    private boolean isServer = false;

    @CommandLine.Option(
            names = {"-a", "--address"},
            description = "The address to bind to.")
    private InetSocketAddress bindAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);

    @CommandLine.Option(
            names = {"-r", "--remote"},
            description = "The address to connect to.")
    private InetSocketAddress remoteAddress;

    private Selector selector;
    private ServerSocketChannel serverSocket;
    private boolean isRunning = true;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        if (!isServer) {
            bindAddress = new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Received shutdown signal");
            isRunning = false;
        }));

        try {
            if (isServer) {
                serve();
            } else {
                connect();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(isRunning) {
            try {
                selector.selectNow();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (SelectionKey key : selector.selectedKeys()) {
                Runnable runnable = (Runnable) key.attachment();

                if(runnable != null) {
                    runnable.run();
                }
            }

            selector.selectedKeys().clear();
        }

        try {
            for (SelectionKey key : selector.keys()) {
                key.cancel();

                if (!(key.channel() instanceof ServerSocketChannel)) {
                    key.channel().close();
                }
            }

            if (isServer) {
                serverSocket.close();
            }

            selector.close();
        } catch (IOException e) {
            LOGGER.warn("Unable to close resources", e);
        }
    }

    private void serve() throws IOException {
        selector = Selector.open();

        serverSocket = ServerSocketChannel.open();
        serverSocket.configureBlocking(false);
        serverSocket.bind(bindAddress);

        SelectionKey key = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        Acceptor acceptor = new Acceptor(selector, serverSocket);
        key.attach(acceptor);
    }

    private void connect() throws IOException {
        selector = Selector.open();

        SocketChannel socket = SocketChannel.open();
        socket.configureBlocking(false);
        socket.connect(remoteAddress);

        SelectionKey key = socket.register(selector, SelectionKey.OP_CONNECT);
        key.attach(new Handler(key, socket));
    }

    public static void main(String... args) {
        CommandLine cli = new CommandLine(new Application());
        cli.registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(DEFAULT_SERVER_PORT));
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        int exitCode = cli.execute(args);

        System.exit(exitCode);
    }
}
