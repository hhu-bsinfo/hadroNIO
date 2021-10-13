package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class SendCallback implements UcxSendCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

    private final HadronioSocketChannel socket;
    private final RingBuffer sendBuffer;

    SendCallback(final HadronioSocketChannel socket, final RingBuffer sendBuffer) {
        this.socket = socket;
        this.sendBuffer = sendBuffer;
    }

    @Override
    public void onSuccess() {
        LOGGER.debug("hadroNIO SendCallback called");
        final AtomicBoolean padding = new AtomicBoolean(true);
        int readFromBuffer;

        do {
            readFromBuffer = sendBuffer.read((msgTypeId, buffer, index, length) -> {
                LOGGER.debug("Message type id: [{}], Index: [{}], Length: [{}]", msgTypeId, index, length);
                padding.set(false);
            }, 1);

            if (padding.get()) {
                LOGGER.debug("Read [{}] padding bytes from send buffer", readFromBuffer);
                sendBuffer.commitRead(readFromBuffer);
            }
        } while (padding.get());

        sendBuffer.commitRead(readFromBuffer);
    }

    @Override
    public void onError() {
        LOGGER.error("hadroNIO SendCallback error handler called (Closing socket channel)");

        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close socket channel (Message: [{}])", e.getMessage());
            LOGGER.debug("Stack trace:", e);
        }
    }
}
