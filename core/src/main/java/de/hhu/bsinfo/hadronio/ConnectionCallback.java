package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionCallback implements UcxSendCallback, UcxReceiveCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

    private final HadronioSocketChannel socket;
    private final AtomicBuffer receiveBuffer;
    private final long localTag;

    ConnectionCallback(final HadronioSocketChannel socket, final AtomicBuffer receiveBuffer, final long localTag) {
        this.socket = socket;
        this.receiveBuffer = receiveBuffer;
        this.localTag = localTag;
    }

    @Override
    public void onMessageSent() {
        LOGGER.debug("Connection callback has been called (Sent tag: [0x{}])", Long.toHexString(localTag));
    }

    @Override
    public void onMessageReceived(long tag) {
        final long remoteTag = receiveBuffer.getLong(0);
        final long checksum = receiveBuffer.getLong(Long.BYTES);
        final long expectedChecksum = TagUtil.calculateChecksum(remoteTag);

        if (checksum == expectedChecksum) {
            LOGGER.debug("Connection callback has been called (Received tag: [0x{}])", Long.toHexString(remoteTag));
            socket.onConnection(true, localTag, remoteTag);
        } else {
            LOGGER.error("Tags have been exchanged, but checksum is wrong (Expected: [0x{}], Received: [0x{}])!", Long.toHexString(expectedChecksum), Long.toHexString(checksum));
            socket.onConnection(false, localTag, 0);
        }
    }
}
