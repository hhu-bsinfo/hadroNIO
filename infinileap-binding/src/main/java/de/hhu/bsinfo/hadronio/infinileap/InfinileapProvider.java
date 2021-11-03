package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxProvider;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ContextParameters;
import de.hhu.bsinfo.infinileap.binding.ContextParameters.Feature;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import java.io.IOException;

public class InfinileapProvider implements UcxProvider {

    private final Context context;

    public InfinileapProvider() throws ControlException {
        context = Context.initialize(new ContextParameters().setFeatures(Feature.WAKEUP, Feature.TAG, Feature.STREAM));
    }

    @Override
    public UcxListener createListener() throws IOException {
        try {
            return new InfinileapListener(context);
        } catch (ControlException e) {
            throw new IOException(e);
        }
    }

    @Override
    public UcxEndpoint createEndpoint() throws IOException {
        try {
            return new InfinileapEndpoint(context);
        } catch (ControlException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        context.close();
    }
}
