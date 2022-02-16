package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import java.net.InetSocketAddress;
import org.openucx.jucx.ucp.UcpConnectionRequest;

class JucxConnectionRequest implements UcxConnectionRequest {

    private final UcpConnectionRequest connectionRequest;

    JucxConnectionRequest(final UcpConnectionRequest connectionRequest) {
        this.connectionRequest = connectionRequest;
    }

    UcpConnectionRequest getConnectionRequest() {
        return connectionRequest;
    }

    @Override
    public void reject() {
        connectionRequest.reject();
    }
}
