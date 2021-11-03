package de.hhu.bsinfo.hadronio;

import java.io.Closeable;

public interface UcxSelectableChannel extends Closeable {

    UcxWorker getWorker();
}
