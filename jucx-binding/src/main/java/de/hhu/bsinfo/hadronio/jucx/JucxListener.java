package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.hadronio.binding.UcxListenerCallback;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

class JucxListener implements UcxListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxListener.class);

    private final UcpContext context;
    private final JucxWorker worker;
    private UcpListener listener;

    JucxListener(final UcpContext context) {
        this.context = context;
        worker = new JucxWorker(context, new UcpWorkerParams());
    }

    @Override
    public void bind(final InetSocketAddress localAddress, final UcxListenerCallback callback) throws IOException {
        final var listenerParams = new UcpListenerParams().setSockAddr(localAddress)
                .setConnectionHandler(request -> callback.onConnectionRequest(new JucxConnectionRequest(request)));

        try {
            listener = worker.getWorker().newListener(listenerParams);
        } catch (UcxException e) {
            throw new IOException("Failed to bind server socket channel to " + localAddress + "!", e);
        }

        LOGGER.info("Listening on [{}]", listener.getAddress());
    }

    @Override
    public UcxEndpoint accept(final UcxConnectionRequest connectionRequest) {
        return new JucxEndpoint(context, ((JucxConnectionRequest) connectionRequest).getConnectionRequest());
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public InetSocketAddress getAddress() {
        if (listener == null) {
            return null;
        } else {
            return listener.getAddress();
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing listener");
        if(listener != null) {
            listener.close();
        }
    }
}
