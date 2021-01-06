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

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxServerSocketChannel.class);

    private final UcpContext context;

    private UcpEndpoint endpoint;
    private UcpWorker worker;

    private boolean connected = false;
    private boolean readable = false;
    private boolean writeable = false;

    protected UcxSocketChannel(SelectorProvider provider) {
        super(provider);

        UcpParams params = new UcpParams().requestWakeupFeature().requestTagFeature();
        context = new UcpContext(params);
        worker = new UcpWorker(context, new UcpWorkerParams().requestThreadSafety());
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpEndpoint endpoint) {
        super(provider);

        this.context = context;
        this.endpoint = endpoint;
    }

    @Override
    public SocketChannel bind(SocketAddress socketAddress) throws IOException {
        /*if (socketAddress == null) {
            localAddress = new InetSocketAddress(UcxProvider.DEFAULT_SERVER_PORT);
        } else if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        } else {
            localAddress = (InetSocketAddress) socketAddress;
        }

        LOGGER.info("Binding socket to {}", localAddress);

        return this;*/

        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        readable = false;

        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        writeable = false;

        return this;
    }

    @Override
    public Socket socket() {
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
        LOGGER.info("Connecting to {}", socketAddress);

        UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) socketAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);

        LOGGER.info("Endpoint created: {}", endpoint);

        /*try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(8);
            worker.progressRequest(endpoint.sendTaggedNonBlocking(buffer, null));
            worker.progressRequest(worker.recvTaggedNonBlocking(buffer, null));
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        return (connected = true);
    }

    @Override
    public boolean finishConnect() throws IOException {
        return connected;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
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
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
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
