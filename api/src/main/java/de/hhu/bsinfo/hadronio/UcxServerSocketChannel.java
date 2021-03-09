package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public interface UcxServerSocketChannel extends HadronioSelectableChannel, Closeable {

    void bind(InetSocketAddress socketAddress, int backlog) throws IOException;

    SocketChannel accept() throws IOException;

    void configureBlocking(boolean blocking);
}
