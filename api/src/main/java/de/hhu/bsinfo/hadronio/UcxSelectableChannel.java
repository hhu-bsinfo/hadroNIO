package de.hhu.bsinfo.hadronio;

import org.openucx.jucx.ucp.UcpWorker;

import java.io.IOException;

public interface UcxSelectableChannel {

    int readyOps();
    void select() throws IOException;

    static void pollWorkerNonBlocking(UcpWorker worker) throws IOException {
        try {
            worker.progress();
        } catch (Exception e) {
            throw new IOException("Failed to progress worker!", e);
        }
    }

    static void pollWorkerBlocking(UcpWorker worker) throws IOException {
        try {
            if (worker.progress() == 0) {
                worker.waitForEvents();
            }
        } catch (Exception e) {
            throw new IOException("Failed to progress worker!", e);
        }
    }
}
