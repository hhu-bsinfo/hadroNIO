package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;

public class UcxSocketChannel extends SocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxSocketChannel.class);

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;

    private UcpEndpoint endpoint;

    private boolean connected = false;
    private boolean readable = false;
    private boolean writeable = false;

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context) {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpConnectionRequest connectionRequest) throws IOException {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());

        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        connected = establishConnection();
    }

    @Override
    public SocketChannel bind(SocketAddress socketAddress) throws IOException {
        LOGGER.warn("Trying to bind socket channel to [{}], but binding is not supported", socketAddress);

        return this;
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        LOGGER.warn("Trying to set option [{}], which is not supported", socketOption.name());

        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
        LOGGER.warn("Trying to get option [{}], which is not supported", socketOption.name());

        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be readable");

        readable = false;

        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be writeable");

        writeable = false;

        return this;
    }

    @Override
    public Socket socket() {
        LOGGER.error("Direct socket access is not supported");

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean connect(SocketAddress remoteAddress) throws IOException {
        LOGGER.info("Connecting to [{}]", remoteAddress);

        if (isBlocking()) {
            connected = connectTo(remoteAddress);
        } else {
            new Thread(() -> {
                connected = connectTo(remoteAddress);
            }, "connector").start();
        }

        return connected;
    }

    private boolean connectTo(SocketAddress remoteAddress) {
        UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        return establishConnection();
    }

    private boolean establishConnection() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(8);

        LOGGER.info("Exchanging small message to establish connection");

        try {
            UcpRequest sendRequest = endpoint.sendTaggedNonBlocking(buffer, null);
            UcpRequest recvRequest = worker.recvTaggedNonBlocking(buffer, null);

            while (!sendRequest.isCompleted() && ! recvRequest.isCompleted()) {
                worker.progress();
            }

            sendRequest.close();
            recvRequest.close();
        } catch (Exception e) {
            LOGGER.error("Failed to establish the connection", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (connected) {
            return true;
        }

        if (isBlocking()) {
            LOGGER.info("Waiting for connection to be established");

            while (!connected) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    close();
                    break;
                }
            }
        }

        return connected;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        LOGGER.warn("Trying to get remote address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        if (!readable) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        if (!readable) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        if (!writeable) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        if (!writeable) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        LOGGER.warn("Trying to get local address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing socket channel");
        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public boolean isAcceptable() {
        return false;
    }

    @Override
    public boolean isConnectable() {
        return connected;
    }

    @Override
    public boolean isReadable() {
        return false;
    }

    @Override
    public boolean isWriteable() {
        return false;
    }
}
