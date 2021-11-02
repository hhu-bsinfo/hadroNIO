package de.hhu.bsinfo.hadronio;

import java.io.IOException;

interface HadronioSelectableChannel {

    int readyOps();

    void select() throws IOException;

    UcxWorker getWorker();
}
