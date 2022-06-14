package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class WrappingSocket extends Socket {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrappingSocket.class);

    private final HadronioSocketChannel channel;

    public WrappingSocket(final HadronioSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void bind(SocketAddress endpoint) throws IOException {
        channel.bind(endpoint);
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        channel.connect(endpoint);
        while (!channel.isConnected()) {
            channel.finishConnect();
        }
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        if (timeout > 0) {
            throw new UnsupportedOperationException("Connection with timeout is not supported!");
        }

        connect(endpoint);
    }

    @Override
    public SocketChannel getChannel() {
        return channel;
    }

    @Override
    public InetAddress getInetAddress() {
        try {
            return ((InetSocketAddress) channel.getRemoteAddress()).getAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public InputStream getInputStream() {
        throw new UnsupportedOperationException("Sending/Receiving via direct socket access is not supported!");
    }

    @Override
    public boolean getKeepAlive() {
        return false;
    }

    @Override
    public InetAddress getLocalAddress() {
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
    public boolean getOOBInline() {
        return false;
    }

    @Override
    public OutputStream getOutputStream() {
        throw new UnsupportedOperationException("Sending/Receiving via direct socket access is not supported!");
    }

    @Override
    public int getPort() {
        try {
            return ((InetSocketAddress) channel.getRemoteAddress()).getPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        return Configuration.getInstance().getBufferSliceLength();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean getReuseAddress() {
        return false;
    }

    @Override
    public int getSendBufferSize() {
        return Configuration.getInstance().getBufferSliceLength();
    }

    @Override
    public int getSoLinger() {
        return -1;
    }

    @Override
    public int getSoTimeout() {
        return 0;
    }

    @Override
    public boolean getTcpNoDelay() {
        return false;
    }

    @Override
    public int getTrafficClass() {
        return 0;
    }

    @Override
    public boolean isBound() {
        return false;
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public boolean isClosed() {
        return !channel.isOpen();
    }

    @Override
    public boolean isInputShutdown() {
        return channel.isInputClosed();
    }

    @Override
    public boolean isOutputShutdown() {
        return channel.isOutputClosed();
    }

    @Override
    public void sendUrgentData(int data) {
        throw new UnsupportedOperationException("Sending/Receiving via direct socket access is not supported!");
    }

    @Override
    public void setKeepAlive(boolean on) {
        LOGGER.warn("Trying to set keep alive to [{}], but setting socket options is not supported!", on);
    }

    @Override
    public void setOOBInline(boolean on) {
        LOGGER.warn("Trying to set oob inline to [{}], but setting socket options is not supported!", on);
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
    public void setSendBufferSize(int size) {
        LOGGER.warn("Trying to set send buffer size to [{}], but setting socket options is not supported!", size);
    }

    @Override
    public void setSoLinger(boolean on, int linger) {
        LOGGER.warn("Trying to set so linger to [{}], but setting socket options is not supported!", on);
    }

    @Override
    public void setSoTimeout(int timeout) {
        LOGGER.warn("Trying to set timeout to [{}], but setting socket options is not supported!", timeout);
    }

    @Override
    public void setTcpNoDelay(boolean on) {
        LOGGER.warn("Trying to set tcp no delay to [{}], but setting socket options is not supported!", on);
    }

    @Override
    public void setTrafficClass(int trafficClass) {
        LOGGER.warn("Trying to set traffic class to [{}], but setting socket options is not supported!", trafficClass);
    }

    @Override
    public void shutdownInput() throws IOException {
        channel.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
        channel.shutdownOutput();
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
