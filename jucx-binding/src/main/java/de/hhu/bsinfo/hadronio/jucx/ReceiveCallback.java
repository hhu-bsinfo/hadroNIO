package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.UcxCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReceiveCallback extends UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final JucxEndpoint endpoint;
    private final UcxReceiveCallback callback;

    public ReceiveCallback(JucxEndpoint endpoint, final UcxReceiveCallback callback) {
        this.endpoint = endpoint;
        this.callback = callback;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("JUCX ReceiveCallback called (Completed: [{}], Size: [{}], Tag: [0x{}])", request.isCompleted(), request.getRecvSize(), Long.toHexString(request.getSenderTag()));
        if (request.isCompleted()) {
            callback.onMessageReceived(request.getSenderTag());
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to receive a message (Status: [{}], Error: [{}])!", ucsStatus, errorMessage);
        endpoint.handleError();
    }
}
