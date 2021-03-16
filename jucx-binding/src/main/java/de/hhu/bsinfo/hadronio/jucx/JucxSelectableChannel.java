package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxSelectableChannel;
import org.openucx.jucx.ucp.UcpWorker;

import java.io.IOException;

public class JucxSelectableChannel implements UcxSelectableChannel {

    private final UcpWorker worker;

    public JucxSelectableChannel(UcpWorker worker) {
        this.worker = worker;
    }

    @Override
    public void pollWorker(final boolean blocking) throws IOException {
        try {
            int events = worker.progress();
            while (blocking && events == 0) {
                worker.waitForEvents();
                events = worker.progress();
            }
        } catch (Exception e) {
            throw new IOException("Failed to progress worker!", e);
        }
    }

    @Override
    public void interruptPolling() {
        worker.signal();
    }

    protected UcpWorker getWorker() {
        return worker;
    }

    @Override
    public void close() throws IOException {
        worker.close();
    }
}
