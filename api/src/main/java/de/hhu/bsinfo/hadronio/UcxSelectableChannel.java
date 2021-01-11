package de.hhu.bsinfo.hadronio;

import java.io.IOException;

public interface UcxSelectableChannel {

    int readyOps();
    void select() throws IOException;
}
