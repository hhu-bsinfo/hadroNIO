package de.hhu.bsinfo.ucx;

import de.hhu.bsinfo.ucx.util.InetSocketAddressConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
        System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.ucx.UcxProvider");
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

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        if (!isServer) {
            bindAddress = new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        try {
            if (isServer) {
                serve();
            } else {
                connect();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(!Thread.interrupted()) {
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
    }

    private void serve() throws IOException {
        selector = Selector.open();

        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(bindAddress);
        serverSocket.configureBlocking(false);

        SelectionKey key = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        key.attach(new Acceptor(serverSocket));
    }

    private void connect() throws IOException {
        selector = Selector.open();

        SocketChannel socket = SocketChannel.open(remoteAddress);
        socket.configureBlocking(false);
    }

    public static void main(String... args) {
        UcxProvider.printBanner();

        CommandLine cli = new CommandLine(new Application());
        cli.registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(DEFAULT_SERVER_PORT));
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        int exitCode = cli.execute(args);

        System.exit(exitCode);
    }

    private static final class Acceptor implements Runnable {

        private final ServerSocketChannel serverSocket;

        private SocketChannel socket;

        private Acceptor(ServerSocketChannel serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            try {
                socket = serverSocket.accept();
                socket.configureBlocking(false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
