package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.*;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private final JucxWorker worker;
    private UcpEndpoint endpoint;
    private InetSocketAddress remoteAddress;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;
    private boolean errorState = false;

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
    public UcxRequest receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var request = worker.getWorker().recvTaggedNonBlocking(address, size, tag, TagUtil.TAG_MASK_FULL, useCallback ? receiveCallback : null);
        if (blocking) {
            handledProgressRequest(request);
        }
        return new JucxRequest(request);
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

    @Override
    public void cancelRequest(final UcxRequest request) {
        worker.getWorker().cancelRequest(((JucxRequest) request).getRequest());
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
        }
        if (worker != null) {
            worker.close();
        }
    }

    void handleError() {
        errorState = true;
    }
}
