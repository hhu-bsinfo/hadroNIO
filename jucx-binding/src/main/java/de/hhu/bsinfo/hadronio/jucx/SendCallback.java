package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SendCallback extends org.openucx.jucx.UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

    private final JucxEndpoint endpoint;
    private final UcxCallback callback;

    public SendCallback(JucxEndpoint endpoint, final UcxCallback callback) {
        this.endpoint = endpoint;
        this.callback = callback;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("JUCX SendCallback called (Completed: [{}])", request.isCompleted());
        if (request.isCompleted()) {
            callback.onSuccess();
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to send a message (Status: [{}], Error: [{}])!", ucsStatus, errorMessage);
        endpoint.handleError();
    }
}
