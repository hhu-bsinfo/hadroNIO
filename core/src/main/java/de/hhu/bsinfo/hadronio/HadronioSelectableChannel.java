package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import java.io.IOException;

interface HadronioSelectableChannel {

    int readyOps();

    void select() throws IOException;

    UcxWorker getWorker();
}
