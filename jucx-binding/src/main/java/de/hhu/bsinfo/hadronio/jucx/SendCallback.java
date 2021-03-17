package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendCallback extends org.openucx.jucx.UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

    private final UcxCallback callback;

    public SendCallback(final UcxCallback callback) {
        this.callback = callback;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("SendCallback called (Completed: [{}])", request.isCompleted());
        if (request.isCompleted()) {
            callback.onSuccess(0);
            request.close();
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to send a message! Status: [{}], Error: [{}]", ucsStatus, errorMessage);
        callback.onError();
    }
}
