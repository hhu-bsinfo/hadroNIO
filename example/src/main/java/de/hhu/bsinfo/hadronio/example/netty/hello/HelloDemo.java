package de.hhu.bsinfo.hadronio.example.netty.hello;

import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "hello",
    description = "Example application, that exchanges a single message between client and server.",
    showDefaultValues = true,
    separator = " ")
public class HelloDemo implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloDemo.class);
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

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        final Runnable runnable = isServer ? new Server(bindAddress) : new Client(bindAddress, remoteAddress);
        runnable.run();
    }
}
