package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.generated.DebugConfig;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionCallback implements UcxSendCallback, UcxReceiveCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

    private final HadronioSocketChannel socket;
    private final AtomicBuffer receiveBuffer;
    private final long localTag;

    private int counter;
    private boolean success;
    private long remoteTag;

    ConnectionCallback(final HadronioSocketChannel socket, final AtomicBuffer receiveBuffer, final long localTag) {
        this.socket = socket;
        this.receiveBuffer = receiveBuffer;
        this.localTag = localTag;
    }

    @Override
    public void onMessageSent() {
        if (DebugConfig.DEBUG) LOGGER.debug("Connection callback has been called (Sent tag: [0x{}], counter: [{}])", Long.toHexString(localTag), counter + 1);
        if (++counter == 2) {
            socket.onConnection(success, localTag, remoteTag);
        }
    }

    @Override
    public void onMessageReceived(long tag) {
        remoteTag = receiveBuffer.getLong(0);
        final long checksum = receiveBuffer.getLong(Long.BYTES);
        final long expectedChecksum = TagUtil.calculateChecksum(remoteTag);

        if (checksum == expectedChecksum) {
            if (DebugConfig.DEBUG) LOGGER.debug("Connection callback has been called (Received tag: [0x{}], counter: [{}])", Long.toHexString(remoteTag), counter + 1);
            success = true;
        } else {
            LOGGER.error("Tags have been exchanged, but checksum is wrong (Expected: [0x{}], Received: [0x{}])!", Long.toHexString(expectedChecksum), Long.toHexString(checksum));
            success = false;
        }

        if (++counter == 2) {
            socket.onConnection(success, localTag, remoteTag);
        }
    }
}
