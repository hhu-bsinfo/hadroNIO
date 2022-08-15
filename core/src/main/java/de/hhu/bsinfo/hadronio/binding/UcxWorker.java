package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;
import java.io.IOException;

public interface UcxWorker extends Closeable {

    boolean progress();

    void waitForEvents();

    void interrupt();

    void arm();

    int getEventFileDescriptor();

    default boolean drain() {
        boolean currentProgress = progress();
        boolean eventsPolled = currentProgress;

        while (currentProgress) {
            currentProgress = progress();
        }

        return eventsPolled;
    }
}
