package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.agrona.concurrent.QueuedPipe;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static de.hhu.bsinfo.hadronio.UcxSocketChannel.HEADER_OFFSET_MESSAGE_LENGTH;
import static de.hhu.bsinfo.hadronio.UcxSocketChannel.HEADER_OFFSET_READ_BYTES;

class ReceiveCallback extends UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final SocketChannel socket;
    private final RingBuffer receiveBuffer;
    private final QueuedPipe<Integer> receiveIndexQueue;
    private final AtomicInteger readableMessages;

    public ReceiveCallback(final SocketChannel socket, final RingBuffer receiverBuffer, final QueuedPipe<Integer> receiveIndexQueue, final AtomicInteger readableMessages) {
        this.socket = socket;
        this.receiveBuffer = receiverBuffer;
        this.receiveIndexQueue = receiveIndexQueue;
        this.readableMessages = readableMessages;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}])", request.isCompleted(), request.getRecvSize());

        if (request.isCompleted()) {
            final Integer index = receiveIndexQueue.poll();
            if (index == null) {
                throw new IllegalStateException("Receive index queue is empty!");
            }
            LOGGER.debug("Buffer index: [{}]", index);

            // Put message length
            receiveBuffer.buffer().putInt(index + HEADER_OFFSET_MESSAGE_LENGTH, (int) request.getRecvSize());
            // Put number of read bytes (initially 0)
            receiveBuffer.buffer().putInt(index + HEADER_OFFSET_READ_BYTES, 0);

            final int readable = readableMessages.incrementAndGet();
            LOGGER.debug("Readable messages left: [{}]", readable);
        }
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
