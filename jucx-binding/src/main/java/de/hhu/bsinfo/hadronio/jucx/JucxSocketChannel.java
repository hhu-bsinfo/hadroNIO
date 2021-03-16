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

public class JucxSocketChannel extends JucxSelectableChannel implements UcxSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxSocketChannel.class);

    static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    static final long CONNECTION_TAG = 0;
    static final long TAG_MASK = 0xffffffffffffffffL;

    private UcpEndpoint endpoint;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;

    private boolean connected = false;

    JucxSocketChannel(final UcpContext context) {
        super(context.newWorker(new UcpWorkerParams().requestThreadSafety()));
    }

    JucxSocketChannel(final UcpContext context, final UcpConnectionRequest connectionRequest) throws IOException {
        super(context.newWorker(new UcpWorkerParams().requestThreadSafety()));
        endpoint = getWorker().newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        LOGGER.info("Endpoint created: [{}]", endpoint);

        establishConnection(null);
        while (!connected) {
            pollWorker(true);
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
    public void connect(final InetSocketAddress remoteAddress, final UcxCallback callback) {
        final UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress(remoteAddress).setPeerErrorHandlingMode();
        endpoint = getWorker().newEndpoint(endpointParams);

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
        getWorker().recvTaggedNonBlocking(receiveBuffer, CONNECTION_TAG, TAG_MASK, connectionCallback);
    }

    @Override
    public void sendTaggedMessage(long address, long size, long tag) {
        endpoint.sendTaggedNonBlocking(address, size, tag, sendCallback);
    }

    @Override
    public void receiveTaggedMessage(long address, long size, long tag, long tagMask) {
        getWorker().recvTaggedNonBlocking(address, size, tag, tagMask, receiveCallback);
    }

    void onConnection(boolean success) {
        connected = success;
    }

    @Override
    public void close() throws IOException {
        endpoint.close();
        super.close();
    }
}
