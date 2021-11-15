package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import de.hhu.bsinfo.hadronio.binding.UcxException;
import org.openucx.jucx.ucp.UcpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCallback extends org.openucx.jucx.UcxCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCallback.class);

    private final UcxCallback callback;

    public StreamCallback(final UcxCallback callback) {
        this.callback = callback;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        LOGGER.debug("JUCX StreamCallback called (Completed: [{}])", request.isCompleted());
        if (request.isCompleted()) {
            callback.onSuccess();
        }
    }

    @Override
    public void onError(final int ucsStatus, final String errorMessage) {
        throw new UcxException("Failed to send/receive data via streaming (Status: [" + ucsStatus + "], Error: [" + errorMessage + "])!");
    }
}
