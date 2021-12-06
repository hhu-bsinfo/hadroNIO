package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpWorker;

import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JucxWorker implements UcxWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxWorker.class);

    private final UcpWorker worker;

    public JucxWorker(final UcpContext context, final UcpWorkerParams workerParams) {
        worker = new UcpWorker(context, workerParams);
    }

    UcpWorker getWorker() {
        return worker;
    }

    @Override
    public boolean progress() {
        try {
            return worker.progress() > 0;
        } catch (Exception e) {
            // Should never happen, since we do no throw exceptions inside our error handlers
            throw new IllegalStateException(e);
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
        LOGGER.info("Closing worker");
        worker.close();
    }
}
