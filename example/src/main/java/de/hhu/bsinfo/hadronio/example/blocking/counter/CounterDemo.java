package de.hhu.bsinfo.hadronio.example.blocking.counter;

import de.hhu.bsinfo.hadronio.HadronioProvider;
import de.hhu.bsinfo.hadronio.util.CloseSignal;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

@CommandLine.Command(
        name = "counter",
        description = "Example application, that sends an increasing counter and receives the other sides counter",
        showDefaultValues = true,
        separator = " ")
public class CounterDemo implements Runnable {

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
        names = {"-c", "--count"},
        description = "The amount of iterations.")
    private int count = 1000;

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
    private SocketChannel socket;

    private int sendCounter;
    private int receiveCounter;

    @Override
    public void run() {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        bindAddress = isServer ? bindAddress : new InetSocketAddress(bindAddress.getAddress(), 0);

        try {
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
        } catch (IOException e) {
            LOGGER.error("Failed to setup connection", e);
            return;
        }

        CloseSignal closeSignal = new CloseSignal(socket);

        try {
            while (sendCounter < count || receiveCounter < count) {
                if (sendCounter < count) {
                    write();
                }

                if (receiveCounter < count) {
                    read();
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to send/receive counter", e);
            return;
        }

        try {
            closeSignal.exchange();
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to exchange close signal", e);
        }
    }

    private void write() throws IOException {
        if (sendBuffer.position() == 0) {
            LOGGER.info("Sending [{}]", ++sendCounter);

            sendBuffer.putInt(sendCounter);
            sendBuffer.rewind();
        }

        socket.write(sendBuffer);

        if (sendBuffer.hasRemaining()) {
            LOGGER.debug("Could not write all bytes at once! Remaining bytes: [{}]", sendBuffer.remaining());
            return;
        }

        sendBuffer.clear();
    }

    private void read() throws IOException {
        socket.read(receiveBuffer);

        if (receiveBuffer.hasRemaining()) {
            LOGGER.debug("Could not read all bytes at once! Remaining bytes: [{}]", receiveBuffer.remaining());
            return;
        }

        receiveBuffer.flip();
        final int counter = receiveBuffer.getInt();

        if (counter != receiveCounter + 1) {
            LOGGER.warn("Counter jump from [{}] to [{}] detected!", receiveCounter, counter);
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close socket channel", e);
            }
        }

        receiveCounter = counter;
        LOGGER.info("Received [{}]", receiveCounter);

        receiveBuffer.clear();
    }
}
