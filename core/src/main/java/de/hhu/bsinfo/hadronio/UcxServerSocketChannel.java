package de.hhu.bsinfo.hadronio;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxServerSocketChannel extends UcxSelectableChannel {

    void bind(InetSocketAddress socketAddress, int backlog) throws IOException;

    UcxSocketChannel accept(UcxConnectionCallback callback) throws IOException;

    boolean hasPendingConnections();

    UcxWorker getWorker();
}
