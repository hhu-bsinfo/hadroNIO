package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxConnectionCallback;
import de.hhu.bsinfo.hadronio.UcxServerSocketChannel;
import de.hhu.bsinfo.hadronio.UcxSocketChannel;
import de.hhu.bsinfo.hadronio.UcxWorker;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Stack;

public class JucxServerSocketChannel implements UcxServerSocketChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxServerSocketChannel.class);

    private final UcpContext context;
    private final JucxWorker worker;
    private final Stack<UcpConnectionRequest> pendingConnections = new Stack<>();
    private UcpListener listener;

    JucxServerSocketChannel(final UcpContext context) {
        this.context = context;
        worker = new JucxWorker(context, new UcpWorkerParams());
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
            listener = worker.getWorker().newListener(listenerParams);
        } catch (UcxException e) {
            throw new IOException("Failed to bind server socket channel to " + localAddress + "!", e);
        }

        LOGGER.info("Listening on [{}]", localAddress);
    }

    @Override
    public UcxSocketChannel accept(final UcxConnectionCallback callback) throws IOException {
        if (pendingConnections.empty()) {
            return null;
        }

        LOGGER.info("Creating new UcxSocketChannel");
        final UcxSocketChannel socket = new JucxSocketChannel(context, pendingConnections.pop(), callback);
        LOGGER.info("Accepted incoming connection");

        return socket;
    }

    @Override
    public boolean hasPendingConnections() {
        return !pendingConnections.isEmpty();
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        listener.close();
    }
}
