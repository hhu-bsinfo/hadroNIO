package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.Configuration;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private static final int MAX_COUNT_OF_WORKER_REQUESTS = Configuration.getInstance().getReceiveBufferLength() / Configuration.getInstance().getBufferSliceLength();
    private final JucxWorker worker;
    private UcpEndpoint endpoint;
    private InetSocketAddress remoteAddress;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;
    private boolean errorState = false;

    private final Queue<UcpRequest> pendingWorkerRequests = new ArrayBlockingQueue<>(MAX_COUNT_OF_WORKER_REQUESTS);

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
        final var request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? sendCallback : null);
        if (blocking) {
            handledProgressRequest(request);
        }

        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        LOGGER.debug("Pending worker requests queue size: [{}]", pendingWorkerRequests.size());
        final var isDefault = TagUtil.getMessageType(tag) == TagUtil.MessageType.DEFAULT;
        final var latestRequest = pendingWorkerRequests.peek();
        if (latestRequest != null && latestRequest.isCompleted()) {
            pendingWorkerRequests.remove();
        }
        if (isDefault && pendingWorkerRequests.size() >= MAX_COUNT_OF_WORKER_REQUESTS) {
            throw new IllegalStateException("Cannot create receive request: pending worker request queue is full");
        }
        final var request =  worker.getWorker().recvTaggedNonBlocking(address, size, tag, TagUtil.TAG_MASK_FULL, useCallback ? receiveCallback : null);
        if (isDefault) {
            pendingWorkerRequests.add(request);
        }
        if (blocking) {
            handledProgressRequest(request);
        }

        return request.isCompleted();
    }

    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var request = endpoint.sendStreamNonBlocking(address, size, useCallback ? sendCallback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
    }

    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var request = endpoint.recvStreamNonBlocking(address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, useCallback ? receiveCallback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        this.sendCallback = new SendCallback(this, sendCallback);
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        this.receiveCallback = new ReceiveCallback(this, receiveCallback);
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
}
