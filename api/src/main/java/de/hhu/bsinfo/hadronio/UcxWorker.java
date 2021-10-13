package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;

public interface UcxWorker extends Closeable {

    void progress() throws IOException;

    void waitForEvents();

    void interrupt();

    default void poll(final boolean blocking) throws IOException {
        if (Configuration.getInstance().useWorkerPollThread()) {
            return;
        }

        progress();

        if (blocking) {
            waitForEvents();
            progress();
        }
    }
}
