package de.hhu.bsinfo.ucx;

import de.hhu.bsinfo.ucx.util.InetSocketAddressConverter;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

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

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        if (!isServer) {
            bindAddress = new InetSocketAddress(bindAddress.getAddress(), 0);
        }

        if (isServer) {
            serve();
        } else {
            connect();
        }
    }

    private void serve() {
        ServerSocketChannel serverSocket;

        try {
            serverSocket = ServerSocketChannel.open();
            serverSocket.bind(bindAddress);
        } catch (IOException e) {
            LOGGER.error("Unable to open server socket channel", e);
        }
    }

    private void connect() {
        UcpParams params = new UcpParams().requestStreamFeature();
        UcpContext context = new UcpContext(params);
        UcpWorker worker = context.newWorker(new UcpWorkerParams());

        UcpEndpointParams epParams = new UcpEndpointParams().setSocketAddress(remoteAddress).setPeerErrorHandlingMode();
        UcpEndpoint clientToServer = worker.newEndpoint(epParams);
    }

    public static void main(String... args) {
        UcxProvider.printBanner();

        CommandLine cli = new CommandLine(new Application());
        cli.registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(DEFAULT_SERVER_PORT));
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        int exitCode = cli.execute(args);

        System.exit(exitCode);
    }
}
