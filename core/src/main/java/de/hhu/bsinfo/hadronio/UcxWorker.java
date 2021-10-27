package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;

public interface UcxWorker extends Closeable {

    boolean progress() throws IOException;

    void waitForEvents();

    void interrupt();
}
