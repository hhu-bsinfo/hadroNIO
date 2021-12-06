package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private final JucxWorker worker;
    private UcpEndpoint endpoint;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;
    private boolean errorState = false;

    JucxEndpoint(final UcpContext context) {
        worker = new JucxWorker(context, new UcpWorkerParams().requestWakeupTagSend().requestWakeupTagRecv());
    }

    JucxEndpoint(final UcpContext context, final UcpConnectionRequest connectionRequest) {
        worker = new JucxWorker(context, new UcpWorkerParams().requestWakeupTagSend().requestWakeupTagRecv());
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

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final UcpRequest request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? sendCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                // Should never happen, since we do no throw exceptions inside our error handlers
                throw new IllegalStateException(e);
            }
        }

        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final long tagMask, final boolean useCallback, final boolean blocking) {
        final UcpRequest request = worker.getWorker().recvTaggedNonBlocking(address, size, tag, tagMask, useCallback ? receiveCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                // Should never happen, since we do no throw exceptions inside our error handlers
                throw new IllegalStateException(e);
            }
        }

        return request.isCompleted();
    }

    @Override
    public void sendStream(final long address, final long size, final UcxCallback callback) {
        endpoint.sendStreamNonBlocking(address, size, new StreamCallback(this, callback));
    }

    @Override
    public void receiveStream(final long address, final long size, final UcxCallback callback) {
        endpoint.recvStreamNonBlocking(address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new StreamCallback(this, callback));
    }

    @Override
    public void setSendCallback(final UcxCallback sendCallback) {
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
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
        endpoint.close();
    }

    void handleError() {
        errorState = true;
    }
}
