package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import java.util.Stack;

public class UcxServerSocketChannel extends ServerSocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxServerSocketChannel.class);

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private final SelectorProvider provider;
    private final UcpContext context;
    private final UcpWorker worker;

    private ConnectionListenerThread listenerThread;

    private InetSocketAddress localAddress;

    protected UcxServerSocketChannel(SelectorProvider provider, UcpContext context) {
        super(provider);

        this.provider = provider;
        this.context = context;

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
    }

    @Override
    public ServerSocketChannel bind(SocketAddress socketAddress, int backlog) throws IOException {
        if (socketAddress == null) {
            localAddress = new InetSocketAddress(UcxProvider.DEFAULT_SERVER_PORT);
        } else if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        } else {
            localAddress = (InetSocketAddress) socketAddress;
        }

        listenerThread = new ConnectionListenerThread(worker, localAddress, pendingConnections, backlog);
        resourceHandler.addResource(listenerThread);

        LOGGER.info("Listening on [{}]", localAddress);

        listenerThread.start();

        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
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
    public ServerSocket socket() {
        LOGGER.error("Direct socket access is not supported");

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketChannel accept() throws IOException {
        if (isBlocking()) {
            while (pendingConnections.empty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException("Thread has been interrupted while waiting for an incoming connection");
                }
            }
        } else if (pendingConnections.empty()) {
            return null;
        }

        LOGGER.info("Creating new UcxSocketChannel");

        UcxSocketChannel socket = new UcxSocketChannel(provider, context, pendingConnections.pop());

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

        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Server socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public boolean isAcceptable() {
        return !pendingConnections.empty();
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

    private static final class ConnectionListenerThread extends Thread implements Closeable {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionListenerThread.class);

        private final ResourceHandler resourceHandler = new ResourceHandler();
        private final UcpWorker worker;

        private boolean isRunning = false;

        private ConnectionListenerThread(UcpWorker worker, InetSocketAddress localAddress, Stack<UcpConnectionRequest> pendingConnections, int backlog) {
            super("listener");

            this.worker = worker;

            UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                    .setConnectionHandler(request -> {
                        LOGGER.info("Received connection request");

                        if (backlog <= 0 || pendingConnections.size() < backlog) {
                            pendingConnections.push(request);
                        } else {
                            LOGGER.error("Discarding connection request, because the maximum number of pending connections ({}) has been reached", backlog);
                        }
                    });

            UcpListener listener = worker.newListener(listenerParams);
            resourceHandler.addResource(listener);
        }

        @Override
        public void run() {
            isRunning = true;

            LOGGER.info("Starting connection listener thread");

            while (isRunning) {
                try {
                    if (worker.progress() == 0) {
                        worker.waitForEvents();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void close() throws IOException {
            LOGGER.info("Stopping connection listener thread");

            this.isRunning = false;
            while(isAlive()) {
                worker.signal();
            }

            resourceHandler.close();
        }
    }
}
