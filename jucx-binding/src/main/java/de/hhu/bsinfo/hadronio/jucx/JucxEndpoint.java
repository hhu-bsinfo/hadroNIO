package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;

class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private final JucxWorker worker;
    private UcpEndpoint endpoint;
    private InetSocketAddress remoteAddress;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;
    private boolean errorState = false;
    private ArrayList<UcpRequest> requests = new ArrayList<>();

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

    private void execRequest(final UcpRequest request, final boolean blocking) {
        if (request.getNativeId() != null) {
            requests.add(request);
        }
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                // Should never happen, since we do no throw exceptions inside our error handlers
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? sendCallback : null);
        execRequest(request, blocking);
        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var request = worker.getWorker().recvTaggedNonBlocking(address, size, tag, TagUtil.TAG_MASK_FULL, useCallback ? receiveCallback : null);
        execRequest(request, blocking);
        return request.isCompleted();
    }

    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var request = endpoint.sendStreamNonBlocking(address, size, useCallback ? sendCallback : null);
        execRequest(request, blocking);
    }

    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var request = endpoint.recvStreamNonBlocking(address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, useCallback ? receiveCallback : null);
        execRequest(request, blocking);
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
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
        if (endpoint != null) {
            endpoint.close();
        }
        for ( UcpRequest request : requests) {
            if (request.getNativeId() != null) {
                worker.getWorker().cancelRequest(request);
            }
        }
    }

    void handleError() {
        errorState = true;
    }
}
