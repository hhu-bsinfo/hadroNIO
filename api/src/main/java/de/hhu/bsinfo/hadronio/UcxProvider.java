package de.hhu.bsinfo.hadronio;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public interface UcxProvider {

    UcxServerSocketChannel createServerSocketChannel();

    UcxSocketChannel createSocketChannel();
}
