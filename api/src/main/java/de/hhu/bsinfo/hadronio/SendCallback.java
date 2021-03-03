package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

class SendCallback extends UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

    private final SocketChannel socket;
    private final RingBuffer sendBuffer;

    public SendCallback(final SocketChannel socket, final RingBuffer sendBuffer) {
        this.socket = socket;
        this.sendBuffer = sendBuffer;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("SendCallback called (Completed: [{}])", request.isCompleted());

        if (request.isCompleted()) {
            final int read = sendBuffer.read((msgTypeId, buffer, index, length) -> {
                LOGGER.debug("Message type id: [{}], Index: [{}], Length: [{}]", msgTypeId, index, length);
            }, 1);

            sendBuffer.commitRead(read);
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to send a message -> Closing socket channel! Status: [{}], Error: [{}]", ucsStatus, errorMessage);

        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close socket channel", e);
        }
    }
}
