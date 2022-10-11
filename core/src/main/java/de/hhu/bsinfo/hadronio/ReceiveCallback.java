package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class ReceiveCallback implements UcxReceiveCallback {

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

        flushBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
        flushBuffer.putLong(0, HadronioSocketChannel.FLUSH_ANSWER);
    }

    @Override
    public void onMessageReceived(long tag) {
        final long id = TagUtil.getTargetId(tag);
        final var messageType = TagUtil.getMessageType(tag);
        LOGGER.debug("hadroNIO ReceiveCallback called (id: [0x{}], messageType: [{}], receiveCouter: [{}])", Long.toHexString(id), messageType, receiveCounter);

        if (messageType == TagUtil.MessageType.FLUSH) {
            LOGGER.debug("Received flush answer");
            isFlushing.set(false);
            return;
        }

        if (++receiveCounter % flushIntervalSize == 0) {
            LOGGER.debug("Sending flush answer");
            final long flushTag = TagUtil.setMessageType(socket.getRemoteTag(), TagUtil.MessageType.FLUSH);
            socket.getSocketChannelImplementation().sendTaggedMessage(flushBuffer.addressOffset(), flushBuffer.capacity(), flushTag, false, true);
        }

        int readable = readableMessages.incrementAndGet();
        LOGGER.debug("Readable messages: [{}]", readable);
    }
}
