package de.hhu.bsinfo.ucx;

import org.openucx.jucx.ucp.UcpEndpoint;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;

public class UcxSocketChannel extends SocketChannel implements UcxSelectableChannel {

    private UcpEndpoint endpoint;

    protected UcxSocketChannel(SelectorProvider provider) {
        super(provider);
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpEndpoint endpoint) {
        super(provider);

        this.endpoint = endpoint;
    }

    @Override
    public SocketChannel bind(SocketAddress socketAddress) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
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
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public Socket socket() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean isConnected() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean connect(SocketAddress socketAddress) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean finishConnect() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    protected void implConfigureBlocking(boolean b) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
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
