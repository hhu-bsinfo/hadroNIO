package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;

class InfinileapConnectionRequest implements UcxConnectionRequest {

    private final InfinileapListener listener;
    private final ConnectionRequest connectionRequest;

    public InfinileapConnectionRequest(final InfinileapListener listener, final ConnectionRequest connectionRequest) {
        this.listener = listener;
        this.connectionRequest = connectionRequest;
    }

    @Override
    public void reject() {
        listener.reject(connectionRequest);
    }

    public ConnectionRequest getConnectionRequest() {
        return connectionRequest;
    }
}
