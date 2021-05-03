package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.*;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.IOException;

public class JucxProvider implements UcxProvider {

    private final UcpContext context;
    private final JucxWorker worker;

    public JucxProvider() {
        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());
        worker = new JucxWorker(context.newWorker(new UcpWorkerParams().requestThreadSafety()));
    }

    @Override
    public UcxServerSocketChannel createServerSocketChannel() {
        return new JucxServerSocketChannel(worker);
    }

    @Override
    public UcxSocketChannel createSocketChannel() {
        return new JucxSocketChannel(worker);
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() throws IOException {
        worker.close();
        context.close();
    }
}
