package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import java.io.IOException;

interface HadronioSelectableChannel {

    void select() throws IOException;

    default void handleError() {};

    int readyOps();

    UcxWorker getWorker();
}
