package de.hhu.bsinfo.hadronio.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class CloseSignal {

    private static final Logger LOGGER = LoggerFactory.getLogger(CloseSignal.class);
    private static final String CLOSE_SIGNAL = "CLOSE";

    private final SocketChannel socket;
    private final ByteBuffer closeBuffer = ByteBuffer.allocateDirect(CLOSE_SIGNAL.getBytes(StandardCharsets.UTF_8).length);

    public CloseSignal(SocketChannel socket) {
        this.socket = socket;
    }

    public void exchange() throws IOException {
        socket.configureBlocking(true);
        socket.write(StandardCharsets.UTF_8.encode(CLOSE_SIGNAL));

        LOGGER.info("Waiting for close signal");
        do {
            socket.read(closeBuffer);
        } while (closeBuffer.hasRemaining());

        LOGGER.info("Received close signal!");

        closeBuffer.flip();
        final String receivedCloseSignal = StandardCharsets.UTF_8.decode(closeBuffer).toString();
        if (!receivedCloseSignal.equals(CLOSE_SIGNAL)) {
            throw new IOException("Got wrong close signal! Expected: [" + CLOSE_SIGNAL + "], Got: [" + receivedCloseSignal + "]");
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted unexpectedly", e);
        }
    }
}
