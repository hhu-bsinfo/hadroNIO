package de.hhu.bsinfo.ucx;

import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final SelectorProvider provider;
    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private final UcpContext context;
    private final UcpWorker worker;

    private ConnectionListenerThread listenerThread;

    private InetSocketAddress localAddress;

    protected UcxServerSocketChannel(SelectorProvider provider) {
        super(provider);

        this.provider = provider;

        LOGGER.info("Initializing ucp context");

        UcpParams params = new UcpParams().requestWakeupFeature();
        context = new UcpContext(params);
        worker = new UcpWorker(context, new UcpWorkerParams().requestThreadSafety());
    }

    @Override
    public ServerSocketChannel bind(SocketAddress socketAddress, int backlog) throws IOException {
        if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

        localAddress = (InetSocketAddress) socketAddress;
        listenerThread = new ConnectionListenerThread(worker, localAddress, pendingConnections, backlog);

        LOGGER.info("Listening on {}", localAddress);

        listenerThread.start();

        return this;
    }

    @Override
    public <T> ServerSocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
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
    public ServerSocket socket() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketChannel accept() throws IOException {
        if (isBlocking()) {
            while (pendingConnections.empty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else if (pendingConnections.empty()) {
            return null;
        }

        UcpEndpointParams endpointParams = new UcpEndpointParams().setConnectionRequest(pendingConnections.pop()).setPeerErrorHandlingMode();
        UcxSocketChannel socket = new UcxSocketChannel(provider, worker.newEndpoint(endpointParams));

        LOGGER.info("Accepted incoming connection!");

        return socket;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return localAddress;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        listenerThread.close();
        worker.close();
        context.close();
    }

    @Override
    protected void implConfigureBlocking(boolean b) throws IOException {

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

    private static final class ConnectionListenerThread extends Thread implements AutoCloseable {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionListenerThread.class);

        private final UcpWorker worker;
        private final UcpListener listener;

        private boolean isRunning = false;

        private ConnectionListenerThread(UcpWorker worker, InetSocketAddress localAddress, Stack<UcpConnectionRequest> pendingConnections, int backlog) {
            this.worker = worker;

            UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                    .setConnectionHandler(request -> {
                        LOGGER.info("Received connection request!");

                        if (backlog <= 0 || pendingConnections.size() < backlog) {
                            pendingConnections.push(request);
                        } else {
                            LOGGER.error("Discarding connection request, because the maximum number of pending connections ({}) has been reached!", backlog);
                        }
                    });

            listener = worker.newListener(listenerParams);
        }

        @Override
        public void run() {
            isRunning = true;

            while (isRunning) {
                if (worker.progress() == 0) {
                    worker.waitForEvents();
                }
            }

            listener.close();
        }

        @Override
        public void close() {
            this.isRunning = false;
            worker.signal();
        }
    }
}
