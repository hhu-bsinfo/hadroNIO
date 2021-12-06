package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.AtomicBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionCallback implements UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

    private final HadronioSocketChannel socket;
    private final AtomicBuffer receiveBuffer;
    private final AtomicInteger successCounter = new AtomicInteger(0);
    private final long localTag;

    ConnectionCallback(final HadronioSocketChannel socket, final AtomicBuffer receiveBuffer, final long localTag) {
        this.socket = socket;
        this.receiveBuffer = receiveBuffer;
        this.localTag = localTag;
    }

    @Override
    public void onSuccess() {
        final int count = successCounter.incrementAndGet();
        LOGGER.debug("Connection callback has been called with a successfully completed request ([{}/2])", count);

        if (count == 2) {
            final long magic = receiveBuffer.getLong(0);
            final long remoteTag = receiveBuffer.getLong(Long.BYTES);

            if (magic == HadronioSocketChannel.CONNECTION_MAGIC_NUMBER) {
                successCounter.set(0);
                socket.onConnection(true, localTag, remoteTag);
            } else {
                LOGGER.error("Connection callback has been called, but magic number is wrong! Expected: [{}], Received: [{}] -> Discarding connection",
                    Long.toHexString(HadronioSocketChannel.CONNECTION_MAGIC_NUMBER), Long.toHexString(magic));
                socket.onConnection(false, localTag, 0);
            }
        }
    }
}
