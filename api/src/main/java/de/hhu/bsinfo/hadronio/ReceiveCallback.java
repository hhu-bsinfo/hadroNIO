package de.hhu.bsinfo.hadronio;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class ReceiveCallback implements UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final HadronioSocketChannel socket;
    private final AtomicInteger readableMessages;
    private final AtomicBoolean isFlushing;
    private final AtomicBuffer flushBuffer;
    private final int flushIntervalSize;

    private int receiveCounter = 0;

    ReceiveCallback(final HadronioSocketChannel socket, final AtomicInteger readableMessages, final AtomicBoolean isFlushing, final int flushIntervalSize) {
        this.socket = socket;
        this.readableMessages = readableMessages;
        this.isFlushing = isFlushing;
        this.flushIntervalSize = flushIntervalSize;

        flushBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(8));
        flushBuffer.putLong(0, HadronioSocketChannel.FLUSH_ANSWER);
    }

    @Override
    public void onSuccess(long tag) {
        if (tag == HadronioSocketChannel.FLUSH_TAG) {
            LOGGER.debug("Received flush answer");
            isFlushing.set(false);
            return;
        }

        if (++receiveCounter % flushIntervalSize == 0 && !socket.isBlocking()) {
            try {
                LOGGER.debug("Sending flush answer");
                socket.getSocketChannelImplementation().sendTaggedMessage(flushBuffer.addressOffset(), flushBuffer.capacity(), HadronioSocketChannel.FLUSH_TAG, false, true);
            } catch (IOException e) {
                LOGGER.error("Failed to send flush message", e);
            }
        }

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
