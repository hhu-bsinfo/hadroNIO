package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;

import static java.nio.channels.SelectionKey.OP_ACCEPT;

public class HadronioServerSocketChannel extends ServerSocketChannel implements HadronioSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioServerSocketChannel.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    private final UcxServerSocketChannel serverSocketChannel;

    private InetSocketAddress localAddress;
    private boolean channelClosed = false;
    private boolean channelBound = false;
    private int readyOps;

    public HadronioServerSocketChannel(final SelectorProvider provider, final UcxServerSocketChannel serverSocketChannel) {
        super(provider);
        this.serverSocketChannel = serverSocketChannel;
    }

    @Override
    public synchronized ServerSocketChannel bind(final SocketAddress socketAddress, final int backlog) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (localAddress != null) {
            throw new AlreadyBoundException();
        }

        if (socketAddress == null) {
            localAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);
        } else if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        } else {
            localAddress = (InetSocketAddress) socketAddress;
        }

        try {
            serverSocketChannel.bind(localAddress, backlog);
            channelBound = true;
        } catch (IOException e){
            channelBound = false;
            throw e;
        }

        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(final SocketOption<T> socketOption, T t) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public <T> T getOption(final SocketOption<T> socketOption) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public ServerSocket socket() {
        try {
            return new WrappingServerSocket(this);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create wrapping server socket!");
        }
    }

    @Override
    public synchronized SocketChannel accept() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (localAddress == null) {
            throw new NotYetBoundException();
        }

        while (isBlocking() && !serverSocketChannel.hasPendingConnections()) {
            serverSocketChannel.getWorker().progress();
        }

        final long[] tags = new long[2];
        final UcxConnectionCallback connectionCallback = (localTag, remoteTag) -> {
            tags[0] = localTag;
            tags[1] = remoteTag;
        };

        final UcxSocketChannel socketChannel = serverSocketChannel.accept(connectionCallback);
        if (socketChannel == null) {
            return null;
        }

        final HadronioSocketChannel ret = new HadronioSocketChannel(provider(), socketChannel);
        ret.onConnection(true, tags[0], tags[1]);
        ret.setConnected();

        return ret;
    }

    @Override
    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing server socket channel bound to [{}]", localAddress);
        channelClosed = true;
        serverSocketChannel.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) {
        LOGGER.info("Server socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public void select() {
        readyOps = serverSocketChannel.hasPendingConnections() ? OP_ACCEPT : 0;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    @Override
    public UcxWorker getWorker() {
        return serverSocketChannel.getWorker();
    }

    boolean isBound() {
        return channelBound;
    }
}
