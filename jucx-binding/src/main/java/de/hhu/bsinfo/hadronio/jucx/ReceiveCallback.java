package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiveCallback extends org.openucx.jucx.UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

    private final long localTag;
    private final UcxCallback callback;

    public ReceiveCallback(final UcxCallback callback, final long localTag) {
        this.localTag = localTag;
        this.callback = callback;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}])", request.isCompleted(), request.getRecvSize());
        if (request.isCompleted()) {
            callback.onSuccess(localTag, request.getSenderTag());
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        LOGGER.error("Failed to receive a message! Status: [{}], Error: [{}]", ucsStatus, errorMessage);
        callback.onError();
    }
}
