package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxServerSocketChannel;
import de.hhu.bsinfo.hadronio.HadronioSocketChannel;
import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Stack;

public class JucxServerSocketChannel implements UcxServerSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxServerSocketChannel.class);
    private static final int DEFAULT_SERVER_PORT = 2998;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private final SelectorProvider provider;
    private final UcpContext context;
    private final UcpWorker worker;

    private final int sendBufferLength;
    private final int receiveBufferLength;
    private final int bufferSliceLength;

    private boolean blocking = true;

    JucxServerSocketChannel(final SelectorProvider provider, final UcpContext context, final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength) {
        this.provider = provider;
        this.context = context;
        this.sendBufferLength = sendBufferLength;
        this.receiveBufferLength = receiveBufferLength;
        this.bufferSliceLength = bufferSliceLength;

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    @Override
    public void bind(InetSocketAddress localAddress, int backlog) throws IOException {
        final UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                .setConnectionHandler(request -> {
                    LOGGER.info("Received connection request");

                    if (backlog <= 0 || pendingConnections.size() < backlog) {
                        pendingConnections.push(request);
                    } else {
                        LOGGER.error("Discarding connection request, because the maximum number of pending connections ({}) has been reached", backlog);
                    }
                });

        try {
            final UcpListener listener = worker.newListener(listenerParams);
            resourceHandler.addResource(listener);
        } catch (UcxException e) {
            throw new IOException("Failed to bind server socket channel to " + localAddress + "!", e);
        }

        LOGGER.info("Listening on [{}]", localAddress);
    }

    @Override
    public SocketChannel accept() throws IOException {
        if (pendingConnections.empty()) {
            if (blocking) {
                while (pendingConnections.empty()) {
                    try {
                        if (worker.progress() == 0) {
                            worker.waitForEvents();
                        }
                    } catch (Exception e) {
                        throw new IOException("Failed to progress worker!", e);
                    }
                }
            } else {
                return null;
            }
        }

        LOGGER.info("Creating new UcxSocketChannel");
        final HadronioSocketChannel socket = new HadronioSocketChannel(provider, new JucxSocketChannel(context, pendingConnections.pop()), sendBufferLength, receiveBufferLength, bufferSliceLength);
        socket.onConnection(true);
        LOGGER.info("Accepted incoming connection");

        return socket;
    }

    @Override
    public void configureBlocking(boolean blocking) {
        this.blocking = blocking;
    }

    @Override
    public void close() throws IOException {
        resourceHandler.close();
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
