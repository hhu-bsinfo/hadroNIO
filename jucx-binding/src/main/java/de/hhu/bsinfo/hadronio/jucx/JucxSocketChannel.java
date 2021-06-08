package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxCallback;
import de.hhu.bsinfo.hadronio.UcxSocketChannel;
import de.hhu.bsinfo.hadronio.util.TagGenerator;
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
    private UcpEndpoint endpoint;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;

    private long localTag;
    private volatile boolean connected = false;

    JucxSocketChannel(final JucxWorker worker) {
        this.worker = worker;
    }

    JucxSocketChannel(final JucxWorker worker, final UcpConnectionRequest connectionRequest, UcxCallback callback) throws IOException {
        this.worker = worker;
        endpoint = worker.getWorker().newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        LOGGER.info("Endpoint created: [{}]", endpoint);

        establishConnection(callback);
        while (!connected) {
            worker.poll(true);
        }
    }

    @Override
    public void setSendCallback(final UcxCallback sendCallback) {
        this.sendCallback = new SendCallback(sendCallback, localTag);
    }

    @Override
    public void setReceiveCallback(final UcxCallback receiveCallback) {
        this.receiveCallback = new ReceiveCallback(receiveCallback, localTag);
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress, final UcxCallback callback) {
        final UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress(remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.getWorker().newEndpoint(endpointParams);

        LOGGER.info("Endpoint created: [{}]", endpoint);
        establishConnection(callback);
    }

    private void establishConnection(final UcxCallback callback) {
        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(2 * Long.BYTES);
        final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(2 * Long.BYTES);

        final int localTag = TagGenerator.generateTag();
        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.putLong(localTag);
        sendBuffer.rewind();

        final ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer, callback, localTag);

        LOGGER.info("Exchanging small message to establish connection");
        endpoint.sendStreamNonBlocking(sendBuffer, connectionCallback);
        endpoint.recvStreamNonBlocking(receiveBuffer, 0, connectionCallback);
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
    public void close() throws IOException {
        endpoint.close();
    }
}
