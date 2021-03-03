package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;
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
import java.util.Stack;

public class UcxServerSocketChannel extends ServerSocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxServerSocketChannel.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private final SelectorProvider provider;
    private final UcpContext context;
    private final UcpWorker worker;

    private final int sendBufferLength;
    private final int receiveBufferLength;
    private final int receiveSliceLength;

    private InetSocketAddress localAddress;

    private boolean channelClosed = false;

    protected UcxServerSocketChannel(SelectorProvider provider, UcpContext context, int sendBufferLength, int receiveBufferLength, int receiveSliceLength) {
        super(provider);

        this.provider = provider;
        this.context = context;
        this.sendBufferLength = sendBufferLength;
        this.receiveBufferLength = receiveBufferLength;
        this.receiveSliceLength = receiveSliceLength;

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    @Override
    public ServerSocketChannel bind(SocketAddress socketAddress, int backlog) throws IOException {
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

        UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                .setConnectionHandler(request -> {
                    LOGGER.info("Received connection request");

                    if (backlog <= 0 || pendingConnections.size() < backlog) {
                        pendingConnections.push(request);
                    } else {
                        LOGGER.error("Discarding connection request, because the maximum number of pending connections ({}) has been reached", backlog);
                    }
                });

        try {
            UcpListener listener = worker.newListener(listenerParams);
            resourceHandler.addResource(listener);
        } catch (UcxException e) {
            throw new IOException("Failed to bind server socket channel to " + localAddress + "!", e);
        }

        LOGGER.info("Listening on [{}]", localAddress);

        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
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
        throw new UnsupportedOperationException("Direct socket access is not supported!");
    }

    @Override
    public SocketChannel accept() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (localAddress == null) {
            throw new NotYetBoundException();
        }

        if (pendingConnections.empty()) {
            if (isBlocking()) {
                while (pendingConnections.empty()) {
                    UcxSelectableChannel.pollWorkerBlocking(worker);
                }
            } else {
                return null;
            }
        }

        LOGGER.info("Creating new UcxSocketChannel");

        UcxSocketChannel socket = new UcxSocketChannel(provider, context, pendingConnections.pop(), sendBufferLength, receiveBufferLength, receiveSliceLength);

        LOGGER.info("Accepted incoming connection");

        return socket;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return localAddress;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing server socket channel bound to [{}]", localAddress);
        channelClosed = true;
        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Server socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        return pendingConnections.empty() ? 0 : SelectionKey.OP_ACCEPT;
    }

    @Override
    public void select() throws IOException {
        try {
            worker.progress();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
