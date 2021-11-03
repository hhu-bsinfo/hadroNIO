package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxListenerCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Listener;
import de.hhu.bsinfo.infinileap.binding.ListenerParameters;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfinileapListener implements UcxListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapListener.class);

    private final Context context;
    private final InfinileapWorker worker;
    private ListenerParameters listenerParameters;
    private Listener listener;

    InfinileapListener(final Context context) throws ControlException {
        this.context = context;
        worker = new InfinileapWorker(context, new WorkerParameters());
    }

    @Override
    public void bind(final InetSocketAddress localAddress, final UcxListenerCallback callback) throws IOException {
        listenerParameters = new ListenerParameters().setListenAddress(localAddress)
                .setConnectionHandler(request -> callback.onConnectionRequest(new InfinileapConnectionRequest(request)));

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
            return new InfinileapEndpoint(context, ((InfinileapConnectionRequest) connectionRequest).getConnectionRequest());
        } catch (ControlException e) {
            throw new IOException(e);
        }
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing listener");
        listener.close();
    }
}
