package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;

public interface UcxProvider extends Closeable {

    UcxListener createListener();

    UcxEndpoint createEndpoint();
}
