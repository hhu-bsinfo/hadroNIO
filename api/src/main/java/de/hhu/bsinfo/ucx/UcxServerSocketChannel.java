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

public class UcxServerSocketChannel extends ServerSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxServerSocketChannel.class);

    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private final UcpContext context;

    private ConnectionListenerThread listenerThread;

    private InetSocketAddress localAddress;

    protected UcxServerSocketChannel(SelectorProvider provider) {
        super(provider);

        LOGGER.info("Initializing ucp context");

        UcpParams params = new UcpParams().requestWakeupFeature();
        context = new UcpContext(params);
    }

    @Override
    public ServerSocketChannel bind(SocketAddress socketAddress, int backlog) throws IOException {
        if (!(socketAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

        localAddress = (InetSocketAddress) socketAddress;
        listenerThread = new ConnectionListenerThread(context, localAddress, pendingConnections, backlog);

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

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return localAddress;
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        listenerThread.close();
        context.close();
    }

    @Override
    protected void implConfigureBlocking(boolean b) throws IOException {

    }

    private static final class ConnectionListenerThread extends Thread implements AutoCloseable {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionListenerThread.class);

        private final UcpWorker worker;
        private final UcpListener listener;

        private boolean isRunning = false;

        private ConnectionListenerThread(UcpContext context, InetSocketAddress localAddress, Stack<UcpConnectionRequest> pendingConnections, int backlog) {
            UcpListenerParams listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                    .setConnectionHandler(request -> {
                        LOGGER.info("Received connection request!");

                        if (backlog <= 0 || pendingConnections.size() < backlog) {
                            pendingConnections.push(request);
                        } else {
                            LOGGER.error("Discarding connection request, because the maximum number of pending connections ({}) has been reached!", backlog);
                        }
                    });

            worker = new UcpWorker(context, new UcpWorkerParams().requestThreadSafety());
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
            worker.close();
        }

        @Override
        public void close() {
            this.isRunning = false;
            worker.signal();
        }
    }
}
