package de.hhu.bsinfo.hadronio;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

class ReceiveCallback extends UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final SocketChannel socket;
    private final AtomicInteger readableMessages;

    public ReceiveCallback(final SocketChannel socket, final AtomicInteger readableMessages) {
        this.socket = socket;
        this.readableMessages = readableMessages;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        int readable = readableMessages.incrementAndGet();
        LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}], Readable messages: [{}])", request.isCompleted(), request.getRecvSize(), readable);
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to receive a message -> Closing socket channel! Status: [{}], Error: [{}]", ucsStatus, errorMessage);

        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close socket channel", e);
        }
    }
}
