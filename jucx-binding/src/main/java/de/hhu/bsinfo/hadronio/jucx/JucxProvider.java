package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.*;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

public class JucxProvider implements UcxProvider {

    private final UcpContext context;
    private final UcpWorker worker;

    public JucxProvider() {
        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());
        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
    }

    @Override
    public UcxServerSocketChannel createServerSocketChannel() {
        return new JucxServerSocketChannel(context, worker);
    }

    @Override
    public UcxSocketChannel createSocketChannel() {
        return new JucxSocketChannel(context, worker);
    }
}
