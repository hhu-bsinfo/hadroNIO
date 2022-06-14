package de.hhu.bsinfo.hadronio.example.grpc.echo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.net.InetSocketAddress;

@CommandLine.Command(
        name = "echo",
        description = "Example application, that implements a simple echo service.",
        showDefaultValues = true,
        separator = " ")
public class EchoDemo implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EchoDemo.class);
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

        final Runnable runnable = isServer ? new Server(bindAddress) : new Client(remoteAddress);
        runnable.run();
    }
}
