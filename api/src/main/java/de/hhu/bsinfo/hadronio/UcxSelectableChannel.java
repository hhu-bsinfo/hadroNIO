package de.hhu.bsinfo.hadronio;

import static java.nio.channels.SelectionKey.*;

public interface UcxSelectableChannel {

    default int readyOps() {
        return (isReadable() ? OP_READ : 0) | (isWriteable() ? OP_WRITE : 0) | (isConnectable() ? OP_CONNECT : 0) | (isAcceptable() ? OP_ACCEPT : 0);
    }

    boolean isAcceptable();
    boolean isConnectable();
    boolean isReadable();
    boolean isWriteable();
}
