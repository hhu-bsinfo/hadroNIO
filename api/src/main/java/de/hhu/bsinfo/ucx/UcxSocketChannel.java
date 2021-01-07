package de.hhu.bsinfo.ucx;

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

    private final UcpContext context;

    private UcpEndpoint endpoint;
    private UcpWorker worker;

    private boolean connected = false;
    private boolean readable = false;
    private boolean writeable = false;

    protected UcxSocketChannel(SelectorProvider provider) {
        super(provider);

        LOGGER.info("Creating ucp context");

        UcpParams params = new UcpParams().requestWakeupFeature().requestTagFeature();
        context = new UcpContext(params);
        worker = new UcpWorker(context, new UcpWorkerParams().requestThreadSafety());
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpEndpoint endpoint) {
        super(provider);

        LOGGER.info("Creating socket channel from existing ucp context");

        this.context = context;
        this.endpoint = endpoint;
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
    public boolean connect(SocketAddress socketAddress) throws IOException {
        LOGGER.info("Connecting to [{}]", socketAddress);

        if (isBlocking()) {
            UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) socketAddress).setPeerErrorHandlingMode();
            endpoint = worker.newEndpoint(endpointParams);
            LOGGER.info("Endpoint created: [{}]", endpoint);

            return (connected = true);
        } else {
            new Thread(() -> {
                UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) socketAddress).setPeerErrorHandlingMode();
                endpoint = worker.newEndpoint(endpointParams);
                LOGGER.info("Endpoint created: [{}]", endpoint);

                connected = true;
            }).start();

            return false;
        }
    }

    @Override
    public boolean finishConnect() throws IOException {
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

        worker.close();
        endpoint.close();
        context.close();
    }

    @Override
    protected void implConfigureBlocking(boolean b) throws IOException {

    }

    @Override
    public boolean isAcceptable() {
        return false;
    }

    @Override
    public boolean isConnectable() {
        return false;
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
