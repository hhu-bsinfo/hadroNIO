package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxListenerCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InfinileapListener implements UcxListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapListener.class);

    private final Context context;
    private final InfinileapWorker worker;
    private Listener listener;
    // Need to be kept as instance variable, or else the connection handler will be garbage collected
    private ListenerParameters listenerParameters;

    InfinileapListener(final Context context) throws ControlException {
        this.context = context;
        worker = new InfinileapWorker(context, new WorkerParameters());
    }

    @Override
    public void bind(final InetSocketAddress localAddress, final UcxListenerCallback callback) throws IOException {
        listenerParameters = new ListenerParameters()
                .setListenAddress(localAddress)
                .setConnectionHandler(new InfinileapConnectionHandler((callback)));

        try {
            listener = worker.getWorker().createListener(listenerParameters);
        } catch (ControlException e) {
            throw new IOException("Failed to bind server socket channel to " + localAddress + "!", e);
        }

        LOGGER.info("Listening on [{}]", localAddress);
    }

    @Override
    public UcxEndpoint accept(final UcxConnectionRequest connectionRequest) throws IOException {
        try {
            return new InfinileapEndpoint(context, ((InfinileapConnectionRequest) connectionRequest).connectionRequest());
        } catch (ControlException e) {
            throw new IOException(e);
        }
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public InetSocketAddress getAddress() {
        // TODO: Implement once available in Infinileap
        return null;
    }

    @Override
    public void close() {
        LOGGER.info("Closing listener");
        listener.close();
    }
}
