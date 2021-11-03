package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;

public class InfinileapConnectionRequest implements UcxConnectionRequest {

    private final ConnectionRequest connectionRequest;

    InfinileapConnectionRequest(final ConnectionRequest connectionRequest) {
        this.connectionRequest = connectionRequest;
    }

    ConnectionRequest getConnectionRequest() {
        return connectionRequest;
    }
}
