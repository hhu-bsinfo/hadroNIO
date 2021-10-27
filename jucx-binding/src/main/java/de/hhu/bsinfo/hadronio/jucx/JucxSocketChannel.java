package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxConnectionCallback;
import de.hhu.bsinfo.hadronio.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.UcxSendCallback;
import de.hhu.bsinfo.hadronio.UcxSocketChannel;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class JucxSocketChannel implements UcxSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxSocketChannel.class);

    static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;

    private final JucxWorker worker;
    private final JucxErrorHandler errorHandler;
    private UcpEndpoint endpoint;
    private UcxCallback sendCallback;
    private UcxCallback receiveCallback;

    private boolean connected = false;

    JucxSocketChannel(final JucxWorker worker) {
        this.worker = worker;
        errorHandler = new JucxErrorHandler(this);
    }

    JucxSocketChannel(final JucxWorker worker, final UcpConnectionRequest connectionRequest, UcxConnectionCallback callback) throws IOException {
        this.worker = worker;
        errorHandler = new JucxErrorHandler(this);
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setConnectionRequest(connectionRequest).
            setPeerErrorHandlingMode().
            setErrorHandler(errorHandler));

        LOGGER.info("Endpoint created: [{}]", endpoint);

        establishConnection(callback);
        while (!connected) {
            worker.progress();
        }
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        this.sendCallback = new SendCallback(sendCallback);
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        this.receiveCallback = new ReceiveCallback(receiveCallback);
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress, final UcxConnectionCallback callback) {
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setSocketAddress(remoteAddress).
            setPeerErrorHandlingMode().
            setErrorHandler(errorHandler));

        LOGGER.info("Endpoint created: [{}]", endpoint);
        establishConnection(callback);
    }

    private void establishConnection(final UcxConnectionCallback callback) {
        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(2 * Long.BYTES);
        final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(2 * Long.BYTES);

        final long localId = TagUtil.generateId();
        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.putLong(localId);
        sendBuffer.rewind();

        final ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer, callback, localId);

        LOGGER.info("Exchanging small message to establish connection");
        endpoint.sendStreamNonBlocking(sendBuffer, connectionCallback);
        endpoint.recvStreamNonBlocking(receiveBuffer, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, connectionCallback);
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) throws IOException {
        final UcpRequest request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? sendCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final long tagMask, final boolean useCallback, final boolean blocking) throws IOException {
        final UcpRequest request = worker.getWorker().recvTaggedNonBlocking(address, size, tag, tagMask, useCallback ? receiveCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return request.isCompleted();
    }

    void onConnection(final boolean success) {
        connected = success;
    }

    @Override
    public void close() {
        connected = false;
        endpoint.close();
    }
}
