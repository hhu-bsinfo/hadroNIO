package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.Configuration;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Queue;

class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private static final int MAX_COUNT_OF_WORKER_REQUESTS = Configuration.getInstance().getReceiveBufferLength() / Configuration.getInstance().getBufferSliceLength();
    private final JucxWorker worker;
    private UcpEndpoint endpoint;
    private InetSocketAddress remoteAddress;
    private UcxSendCallback sendCallback;
    private UcxReceiveCallback receiveCallback;
    private boolean errorState = false;

    private final Queue<UcpRequest> pendingWorkerRequests = new OneToOneConcurrentArrayQueue<>(MAX_COUNT_OF_WORKER_REQUESTS);

    JucxEndpoint(final UcpContext context) {
        worker = new JucxWorker(context, new UcpWorkerParams());
    }

    JucxEndpoint(final UcpContext context, final UcpConnectionRequest connectionRequest) {
        remoteAddress = connectionRequest.getClientAddress();
        worker = new JucxWorker(context, new UcpWorkerParams());
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setConnectionRequest(connectionRequest).
            setPeerErrorHandlingMode().
            setErrorHandler((endpoint, status, message) -> {
                LOGGER.error("A UCX error occurred (Status: [{}], Error: [{}])!", status, message);
                handleError();
            }));

        LOGGER.info("Endpoint created: [{}]", endpoint);
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setSocketAddress(remoteAddress).
            setPeerErrorHandlingMode().
            setErrorHandler((endpoint, status, message) -> {
                LOGGER.error("A UCX error occurred (Status: [{}], Error: [{}])!", status, message);
                handleError();
            }));

        LOGGER.info("Endpoint created: [{}]", endpoint);
    }

    private void handledProgressRequest(final UcpRequest request) {
        try {
            worker.getWorker().progressRequest(request);
        } catch (Exception e) {
            // Should never happen, since we do no throw exceptions inside our error handlers
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var callback = new SendCallback(this, sendCallback);
        final var request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? callback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        LOGGER.debug("Pending worker requests queue size: [{}]", pendingWorkerRequests.size());
        final var messageType = TagUtil.getMessageType(tag);
        final UcpRequest request;
        if (messageType == TagUtil.MessageType.DEFAULT) {
            LOGGER.debug("Pending worker requests queue size: [{}]", pendingWorkerRequests.size());
            final UcxCallback callback;
            if (useCallback) {
                callback = new WorkerReceiveCallback(this, receiveCallback);
            } else {
                callback = new WorkerReceiveCallback(this, null);
            }
            request =  worker.getWorker().recvTaggedNonBlocking(address, size, tag, TagUtil.TAG_MASK_FULL, callback);
            if (!request.isCompleted()) {
                pendingWorkerRequests.add(request);
            }
        } else {
            final var callback = new ReceiveCallback(this, receiveCallback);
            request =  worker.getWorker().recvTaggedNonBlocking(address, size, tag, TagUtil.TAG_MASK_FULL, useCallback ? callback : null);
        }

        if (blocking) {
            handledProgressRequest(request);
        }

        return request.isCompleted();
    }

    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var callback = new SendCallback(this, sendCallback);
        final var request = endpoint.sendStreamNonBlocking(address, size, useCallback ? callback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
    }

    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var callback = new ReceiveCallback(this, receiveCallback);
        final var request = endpoint.recvStreamNonBlocking(address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, useCallback ? callback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        this.sendCallback = sendCallback;
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        this.receiveCallback = receiveCallback;
    }

    @Override
    public boolean getErrorState() {
        return errorState;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return endpoint.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return endpoint.getRemoteAddress();
    }

    private void cancelRequest(final UcpRequest request) {
        worker.getWorker().cancelRequest(request);
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
        if (endpoint != null) {
            final var closeRequest = endpoint.closeNonBlockingForce();
            handledProgressRequest(closeRequest);
            while (!pendingWorkerRequests.isEmpty()) {
                final var request = pendingWorkerRequests.remove();
                if (!request.isCompleted()) {
                    cancelRequest(request);
                }
            }
        }
    }

    void handleError() {
        errorState = true;
    }

    private void removeCompleteWorkerRequest() {
        if (!pendingWorkerRequests.isEmpty()) {
            pendingWorkerRequests.remove();
        }
    }

    private static class WorkerReceiveCallback extends ReceiveCallback {

        public WorkerReceiveCallback(final JucxEndpoint endpoint, final UcxReceiveCallback callback) {
            super(endpoint, callback);
        }

        @Override
        public void onSuccess(final UcpRequest request) {
            getEndpoint().removeCompleteWorkerRequest();
            if (hasCallback()) {
                super.onSuccess(request);
            }
        }

        @Override
        public void onError(final int ucsStatus, final String errorMessage) {
            getEndpoint().removeCompleteWorkerRequest();
            super.onError(ucsStatus, errorMessage);
        }
    }
}
