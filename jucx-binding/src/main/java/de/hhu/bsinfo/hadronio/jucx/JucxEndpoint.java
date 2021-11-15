package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import java.nio.ByteBuffer;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class JucxEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxEndpoint.class);

    private final JucxWorker worker;
    private final JucxErrorHandler errorHandler;
    private UcpEndpoint endpoint;
    private org.openucx.jucx.UcxCallback sendCallback;
    private org.openucx.jucx.UcxCallback receiveCallback;

    private boolean closed = false;

    JucxEndpoint(final UcpContext context) {
        worker = new JucxWorker(context, new UcpWorkerParams().requestWakeupTagSend().requestWakeupTagRecv());
        errorHandler = new JucxErrorHandler(this);
    }

    JucxEndpoint(final UcpContext context, final UcpConnectionRequest connectionRequest) {
        worker = new JucxWorker(context, new UcpWorkerParams().requestWakeupTagSend().requestWakeupTagRecv());
        errorHandler = new JucxErrorHandler(this);
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setConnectionRequest(connectionRequest).
            setPeerErrorHandlingMode().
            setErrorHandler(errorHandler));

        LOGGER.info("Endpoint created: [{}]", endpoint);
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress) {
        endpoint = worker.getWorker().newEndpoint(
            new UcpEndpointParams().
            setSocketAddress(remoteAddress).
            setPeerErrorHandlingMode().
            setErrorHandler(errorHandler));

        LOGGER.info("Endpoint created: [{}]", endpoint);
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) throws IOException {
        final UcpRequest request = endpoint.sendTaggedNonBlocking(address, size, tag, useCallback ? sendCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return request.isCompleted();
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final long tagMask, final boolean useCallback, final boolean blocking) throws IOException {
        final UcpRequest request = worker.getWorker().recvTaggedNonBlocking(address, size, tag, tagMask, useCallback ? receiveCallback : null);
        while (blocking && !request.isCompleted()) {
            try {
                worker.getWorker().progressRequest(request);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return request.isCompleted();
    }

    @Override
    public void sendStream(final long address, final long size, final UcxCallback callback) {
        endpoint.sendStreamNonBlocking(address, size, new StreamCallback(callback));
    }

    @Override
    public void receiveStream(final long address, final long size, final UcxCallback callback) {
        endpoint.recvStreamNonBlocking(address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new StreamCallback(callback));
    }

    @Override
    public void setSendCallback(final UcxCallback sendCallback) {
        this.sendCallback = new SendCallback(sendCallback);
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        this.receiveCallback = new ReceiveCallback(receiveCallback);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
        closed = true;
        endpoint.close();
    }
}
