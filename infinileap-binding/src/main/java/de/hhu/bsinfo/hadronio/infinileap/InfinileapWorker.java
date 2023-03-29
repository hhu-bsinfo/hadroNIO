package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InfinileapWorker implements UcxWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapWorker.class);

    private final Worker worker;

    public InfinileapWorker(final Context context, final WorkerParameters workerParameters) throws ControlException {
        this.worker = context.createWorker(workerParameters);
    }

    Worker getWorker() {
        return worker;
    }

    @Override
    public synchronized boolean progress() {
        return worker.progress() == WorkerProgress.ACTIVE;
    }

    @Override
    public void waitForEvents() {
        worker.await();
    }

    @Override
    public void interrupt() {
        worker.signal();
    }

    @Override
    public boolean arm() {
        return worker.arm() == Status.OK;
    }

    @Override
    public int getEventFileDescriptor() {
        try {
            return worker.fileDescriptor().intValue();
        } catch (ControlException e) {
            throw new IllegalStateException("Failed to get file descriptor from worker!", e);
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing worker");
        worker.close();
    }
}
