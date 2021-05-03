package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;

public interface UcxWorker extends Closeable {

    boolean progress() throws IOException;

    void waitForEvents();

    void interrupt();

    default boolean poll(final boolean blocking) throws IOException {
        boolean eventsPolled = progress();
        if (blocking && !eventsPolled) {
            waitForEvents();
            eventsPolled = progress();
        }

        return eventsPolled;
    }
}
