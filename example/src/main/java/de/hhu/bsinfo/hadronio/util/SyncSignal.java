package de.hhu.bsinfo.hadronio.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class SyncSignal {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncSignal.class);
    private static final String SYNC_SIGNAL = "SYNC";

    private final SocketChannel socket;
    private final ByteBuffer closeBuffer = ByteBuffer.allocateDirect(SYNC_SIGNAL.getBytes(StandardCharsets.UTF_8).length);

    public SyncSignal(SocketChannel socket) {
        this.socket = socket;
    }

    public void exchange() throws IOException {
        socket.configureBlocking(true);
        socket.write(StandardCharsets.UTF_8.encode(SYNC_SIGNAL));

        LOGGER.info("Waiting for sync signal");
        do {
            socket.read(closeBuffer);
        } while (closeBuffer.hasRemaining());
        LOGGER.info("Received sync signal!");

        closeBuffer.flip();
        final var receivedCloseSignal = StandardCharsets.UTF_8.decode(closeBuffer).toString();
        if (!receivedCloseSignal.equals(SYNC_SIGNAL)) {
            throw new IOException("Got wrong close signal! Expected: [" + SYNC_SIGNAL + "], Got: [" + receivedCloseSignal + "]");
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted unexpectedly", e);
        }
    }
}
