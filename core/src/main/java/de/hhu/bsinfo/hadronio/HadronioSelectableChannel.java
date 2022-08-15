package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import io.helins.linux.epoll.Epoll;

interface HadronioSelectableChannel {

    void select();

    int readyOps();

    UcxWorker getWorker();
}
