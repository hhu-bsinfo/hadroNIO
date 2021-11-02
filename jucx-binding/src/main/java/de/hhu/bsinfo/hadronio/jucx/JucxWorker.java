package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxWorker;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpWorker;

import java.io.IOException;
import org.openucx.jucx.ucp.UcpWorkerParams;

public class JucxWorker implements UcxWorker {

    private final UcpWorker worker;
    private final Object progressLock = new Object();

    public JucxWorker(final UcpContext context, final UcpWorkerParams workerParams) {
        worker = new UcpWorker(context, workerParams);
    }

    UcpWorker getWorker() {
        return worker;
    }

    @Override
    public boolean progress() throws IOException {
        try {
            synchronized (progressLock) {
                return worker.progress() > 0;
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void waitForEvents() {
        worker.waitForEvents();
    }

    @Override
    public void interrupt() {
        worker.signal();
    }

    @Override
    public void close() {
        worker.close();
    }
}
