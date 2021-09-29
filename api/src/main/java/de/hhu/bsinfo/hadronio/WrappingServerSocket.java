package de.hhu.bsinfo.hadronio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WrappingServerSocket extends ServerSocket {

    private final HadronioServerSocketChannel channel;

    public WrappingServerSocket(final HadronioServerSocketChannel channel) throws IOException {
        this.channel = channel;
    }

    @Override
    public Socket accept() throws IOException {
        return channel.accept().socket();
    }

    @Override
    public void bind(SocketAddress endpoint) throws IOException {
        channel.bind(endpoint);
    }

    @Override
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
        channel.bind(endpoint, backlog);
    }

    @Override
    public ServerSocketChannel getChannel() {
        return channel;
    }

    @Override
    public InetAddress getInetAddress() {
        try {
            return ((InetSocketAddress) channel.getLocalAddress()).getAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int getLocalPort() {
        try {
            return ((InetSocketAddress) channel.getLocalAddress()).getPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        return Configuration.getInstance().getReceiveBufferLength();
    }

    @Override
    public boolean getReuseAddress() {
        return false;
    }

    @Override
    public int getSoTimeout() {
        return 0;
    }

    @Override
    public boolean isBound() {
        return channel.isBound();
    }

    @Override
    public boolean isClosed() {
        return !channel.isOpen();
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
        throw new UnsupportedOperationException("Setting performance preferences is not supported!");
    }

    @Override
    public void setReceiveBufferSize(int size) {
        throw new UnsupportedOperationException("Setting receive buffer size is not supported!");
    }

    @Override
    public void setReuseAddress(boolean on) {
        throw new UnsupportedOperationException("Setting reuse address is not supported!");
    }

    @Override
    public void setSoTimeout(int timeout) {
        throw new UnsupportedOperationException("Setting so timeout is not supported!");
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
