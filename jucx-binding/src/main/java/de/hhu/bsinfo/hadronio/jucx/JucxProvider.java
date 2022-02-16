package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxListener;
import de.hhu.bsinfo.hadronio.binding.UcxProvider;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

public class JucxProvider implements UcxProvider {

    private final UcpContext context;

    public JucxProvider() {
        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature().requestStreamFeature().setMtWorkersShared(true));
    }

    @Override
    public UcxListener createListener() {
        return new JucxListener(context);
    }

    @Override
    public UcxEndpoint createEndpoint() {
        return new JucxEndpoint(context);
    }

    @Override
    public void close() {
        context.close();
    }
}
