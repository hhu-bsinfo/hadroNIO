package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxServerSocketChannel extends Closeable {

    void bind(InetSocketAddress socketAddress, int backlog) throws IOException;

    UcxSocketChannel accept() throws IOException;

    boolean hasPendingConnections();
}
