package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxConnectionCallback;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static de.hhu.bsinfo.hadronio.jucx.JucxSocketChannel.CONNECTION_MAGIC_NUMBER;

public class ConnectionCallback extends UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

    private final JucxSocketChannel socket;
    private final ByteBuffer receiveBuffer;
    private final UcxConnectionCallback callback;
    private final AtomicInteger successCounter = new AtomicInteger(0);
    private final long localTag;

    ConnectionCallback(final JucxSocketChannel socket, final ByteBuffer receiveBuffer, final UcxConnectionCallback callback, final long localTag) {
        this.socket = socket;
        this.receiveBuffer = receiveBuffer;
        this.callback = callback == null ? (tag, remoteTag) -> {} : callback;
        this.localTag = localTag;
    }

    public void onSuccess(final UcpRequest request) {
        if (request.isCompleted()) {
            final int count = successCounter.incrementAndGet();
            LOGGER.debug("Connection callback has been called with a successfully completed request ([{}/2])", count);

            if (count == 2) {
                final long magic = receiveBuffer.getLong();
                final long remoteTag = receiveBuffer.getLong();

                if (magic == CONNECTION_MAGIC_NUMBER) {
                    successCounter.set(0);
                    socket.onConnection(true);
                    callback.onSuccess(localTag, remoteTag);
                } else {
                    LOGGER.error("Connection callback has been called, but magic number is wrong! Expected: [{}], Received: [{}] -> Discarding connection", Long.toHexString(CONNECTION_MAGIC_NUMBER), Long.toHexString(magic));
                    socket.onConnection(false);
                    callback.onError();
                }
            }
        }
    }

    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to establish connection! Status: [{}], Error: [{}]", ucsStatus, errorMessage);
        callback.onError();
    }
}
