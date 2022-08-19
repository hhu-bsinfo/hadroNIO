package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxListenerCallback;
import de.hhu.bsinfo.infinileap.binding.ConnectionHandler;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;

public class InfinileapConnectionHandler extends ConnectionHandler {

    private final UcxListenerCallback callback;

    public InfinileapConnectionHandler(UcxListenerCallback callback) {
        this.callback = callback;
    }

    @Override
    protected void onConnection(ConnectionRequest connectionRequest) {
        callback.onConnectionRequest(new InfinileapConnectionRequest(connectionRequest));
    }
}
