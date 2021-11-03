package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.binding.WorkerProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfinileapWorker implements UcxWorker {

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
    public void close() {
        LOGGER.info("Closing worker");
        worker.close();
    }
}
