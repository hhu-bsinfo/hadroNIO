package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxWorker;
import org.openucx.jucx.ucp.UcpWorker;

import java.io.IOException;

public class JucxWorker implements UcxWorker {

    private final UcpWorker worker;
    private final Object progressLock = new Object();

    public JucxWorker(final UcpWorker worker) {
        this.worker = worker;
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
