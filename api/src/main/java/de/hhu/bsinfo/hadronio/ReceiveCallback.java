package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

class ReceiveCallback implements UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final SocketChannel socket;
    private final AtomicInteger readableMessages;

    ReceiveCallback(final SocketChannel socket, final AtomicInteger readableMessages) {
        this.socket = socket;
        this.readableMessages = readableMessages;
    }

    @Override
    public void onSuccess() {
        int readable = readableMessages.incrementAndGet();
        LOGGER.debug("Readable messages: [{}]", readable);
    }

    @Override
    public void onError() {
        LOGGER.error("Closing socket channel!");

        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close socket channel", e);
        }
    }
}
