package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;

public interface UcxWorker extends Closeable {

    boolean progress();

    void waitForEvents();

    void interrupt();
}
