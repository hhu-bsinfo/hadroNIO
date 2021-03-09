package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxCallback;
import de.hhu.bsinfo.hadronio.UcxSocketChannel;
import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class JucxSocketChannel implements UcxSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxSocketChannel.class);

    static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    static final long CONNECTION_TAG = 0;
    static final long TAG_MASK = 0xffffffffffffffffL;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;

    private boolean connected = false;
    private boolean connectionPending = false;
    private boolean connectionFailed = false;
    private boolean blocking = true;

    JucxSocketChannel(final UcpContext context) {
        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    JucxSocketChannel(final UcpContext context, final UcpConnectionRequest connectionRequest) throws IOException {
        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);
        LOGGER.info("Endpoint created: [{}]", endpoint);

        connectionPending = true;
        establishConnection(null);

        if (blocking) {
            if (!finishConnect()) {
                throw new IOException("Failed to connect socket channel!");
            }
        }

        if (connected) {
            connectionPending = false;
        }
    }

    @Override
    public void setSendCallback(UcxCallback sendCallback) {
        this.sendCallback = new SendCallback(sendCallback);
    }

    @Override
    public void setReceiveCallback(UcxCallback receiveCallback) {
        this.receiveCallback = new ReceiveCallback(receiveCallback);
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isConnectionPending() {
        return connectionPending;
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress, final UcxCallback callback) throws IOException {
        connectionPending = true;
        connectTo(remoteAddress, callback);

        if (blocking) {
            if (!finishConnect()) {
                throw new IOException("Failed to connect socket channel!");
            }
        }

        if (connected) {
            connectionPending = false;
        }

    }

    private void connectTo(final InetSocketAddress remoteAddress, UcxCallback callback) {
        final UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress(remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);
        establishConnection(callback);
    }

    private void establishConnection(UcxCallback callback) {
        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(8);
        final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(8);

        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.rewind();

        final ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer, callback);

        LOGGER.info("Exchanging small message to establish connection");
        endpoint.sendTaggedNonBlocking(sendBuffer, CONNECTION_TAG, connectionCallback);
        worker.recvTaggedNonBlocking(receiveBuffer, CONNECTION_TAG, TAG_MASK, connectionCallback);
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (connectionFailed) {
            throw new IOException("Failed to connect socket channel!");
        }

        if (connected) {
            connectionPending = false;
            return true;
        } else if (!blocking) {
            return false;
        } else {
            LOGGER.info("Waiting for connection to be established");
            do {
                pollWorkerNonBlocking();

                if (connectionFailed) {
                    throw new IOException("Failed to connect socket channel!");
                }
            } while (!connected);

            connectionPending = false;
            return true;
        }
    }

    @Override
    public void sendTaggedMessage(long address, long size, long tag) {
        endpoint.sendTaggedNonBlocking(address, size, tag, sendCallback);
    }

    @Override
    public void receiveTaggedMessage(long address, long size, long tag, long tagMask) {
        worker.recvTaggedNonBlocking(address, size, tag, tagMask, receiveCallback);
    }

    @Override
    public void configureBlocking(boolean blocking) {
        this.blocking = blocking;
    }

    @Override
    public void pollWorkerBlocking() throws IOException {
        try {
            if (worker.progress() == 0) {
                worker.waitForEvents();
            }
        } catch (Exception e) {
            throw new IOException("Failed to progress worker!", e);
        }
    }

    @Override
    public void pollWorkerNonBlocking() throws IOException {
        try {
            worker.progress();
        } catch (Exception e) {
            throw new IOException("Failed to progress worker!", e);
        }
    }

    @Override
    public void interruptPolling() {
        worker.signal();
    }

    void onConnection(boolean success) {
        connected = success;
        connectionFailed = !success;
    }

    @Override
    public void close() throws IOException {
        resourceHandler.close();
    }
}
