package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;

public interface UcxProgressable extends Closeable {

    UcxWorker getWorker();
}
