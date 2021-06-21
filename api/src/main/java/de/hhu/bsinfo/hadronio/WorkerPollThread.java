package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

class WorkerPollThread extends Thread implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerPollThread.class);

    private final UcxWorker worker;
    private boolean isRunning = false;

    WorkerPollThread(UcxWorker worker) {
        super("WorkerPollThread");
        this.worker = worker;
    }

    @Override
    public void run() {
        isRunning = true;
        LOGGER.info("Starting worker poll thread");

        while (isRunning) {
            try {
                boolean eventsPolled = worker.progress();
                if (!eventsPolled) {
                    worker.waitForEvents();
                }
            } catch (IOException e) {
                LOGGER.error("Failed to poll worker (Message: [{}])", e.getMessage());
                LOGGER.debug("Stack trace:", e);
            }
        }

        LOGGER.info("Worker poll thread has finished");
    }

    @Override
    public void close() {
        LOGGER.info("Stopping worker poll thread");
        isRunning = false;
        worker.interrupt();
    }
}
