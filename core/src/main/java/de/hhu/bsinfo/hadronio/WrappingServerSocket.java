package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;

public class WrappingServerSocket extends ServerSocket {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrappingServerSocket.class);

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
        return ((InetSocketAddress) channel.getLocalAddress()).getAddress();
    }

    @Override
    public int getLocalPort() {
        return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        return channel.getLocalAddress();
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
        LOGGER.warn("Trying to set performance preferences to [connectionTime: [{}], latency: [{}], bandwidth[{}]], but setting socket options is not supported!", connectionTime, latency, bandwidth);
    }

    @Override
    public void setReceiveBufferSize(int size) {
        LOGGER.warn("Trying to set receive buffer size to [{}], but setting socket options is not supported!", size);
    }

    @Override
    public void setReuseAddress(boolean on) {
        LOGGER.warn("Trying to set reuse address to [{}], but setting socket options is not supported!", on);
    }

    @Override
    public void setSoTimeout(int timeout) {
        LOGGER.warn("Trying to set timeout to [{}], but setting socket options is not supported!", timeout);
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
