package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;

public interface UcxSelectableChannel extends Closeable {

    void pollWorker(boolean blocking) throws IOException;
}
