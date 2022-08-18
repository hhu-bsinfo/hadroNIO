package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import java.util.Stack;

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

    private final UcxListener listener;
    private final Stack<UcxConnectionRequest> pendingRequests = new Stack<>();

    private boolean channelClosed = false;
    private boolean channelBound = false;
    private int readyOps;

    public HadronioServerSocketChannel(final SelectorProvider provider, final UcxListener listener) {
        super(provider);
        this.listener = listener;
    }

    @Override
    public synchronized ServerSocketChannel bind(final SocketAddress socketAddress, final int backlog) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (channelBound) {
            throw new AlreadyBoundException();
        }

        InetSocketAddress localAddress;
        if (socketAddress == null) {
            localAddress = new InetSocketAddress(DEFAULT_SERVER_PORT);
        } else if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        } else {
            localAddress = (InetSocketAddress) socketAddress;
        }

        try {
            listener.bind(localAddress, connectionRequest -> {
                LOGGER.info("Received connection request");

                if (backlog <= 0 || pendingRequests.size() < backlog) {
                    pendingRequests.push(connectionRequest);
                } else {
                    LOGGER.error("Discarding connection request, because the maximum number of pending requests ({}) has been reached", backlog);
                    connectionRequest.reject();
                }
            });

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

        if (!channelBound) {
            throw new NotYetBoundException();
        }

        if (!isBlocking() && pendingRequests.isEmpty()) {
            return null;
        }

        while (isBlocking() && pendingRequests.isEmpty()) {
            listener.getWorker().progress();
        }

        LOGGER.info("Accepting connection request");
        final var endpoint = listener.accept(pendingRequests.pop());
        final var socket = new HadronioSocketChannel(provider(), endpoint);

        socket.establishConnection();
        while (!socket.isConnected()) {
            socket.getWorker().progress();
        }

        return socket;
    }

    @Override
    public SocketAddress getLocalAddress() {
        return listener.getAddress();
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing server socket channel bound to [{}]", getLocalAddress());
        channelClosed = true;
        listener.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) {
        LOGGER.info("Server socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public void select() {
        readyOps = pendingRequests.isEmpty() ? 0 : OP_ACCEPT;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    @Override
    public UcxWorker getWorker() {
        return listener.getWorker();
    }

    boolean isBound() {
        return channelBound;
    }
}
