package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;
import java.io.IOException;

public interface UcxProvider extends Closeable {

    UcxListener createListener() throws IOException;

    UcxEndpoint createEndpoint() throws IOException;
}
