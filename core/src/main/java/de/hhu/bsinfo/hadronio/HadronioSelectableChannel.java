package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;

interface HadronioSelectableChannel {

    void select();

    int readyOps();

    UcxWorker getWorker();
}
