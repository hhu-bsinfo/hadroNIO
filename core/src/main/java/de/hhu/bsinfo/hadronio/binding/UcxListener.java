package de.hhu.bsinfo.hadronio.binding;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxListener extends UcxProgressable {

    void bind(InetSocketAddress socketAddress, UcxListenerCallback callback) throws IOException;

    UcxEndpoint accept(UcxConnectionRequest connectionRequest);
}
