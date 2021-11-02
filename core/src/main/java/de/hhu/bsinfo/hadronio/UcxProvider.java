package de.hhu.bsinfo.hadronio;

import java.io.Closeable;

public interface UcxProvider extends Closeable {

    UcxServerSocketChannel createServerSocketChannel();

    UcxSocketChannel createSocketChannel();
}
