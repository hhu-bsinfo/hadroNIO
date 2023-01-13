package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.primitive.NativeLong;
import java.io.IOException;
import java.lang.foreign.MemorySession;
import java.net.InetSocketAddress;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.openucx.Communication.*;

class InfinileapEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapEndpoint.class);

    private static final Tag TAG_MASK_FULL = Tag.of(TagUtil.TAG_MASK_FULL);

    private Endpoint endpoint;
    private final InfinileapWorker worker;
    private InetSocketAddress remoteAddress;

    private final MemorySegment tagInfo = MemorySegment.allocateNative(2 * Long.BYTES, MemorySession.openImplicit());
    private UcxSendCallback sendCallback;
    private UcxReceiveCallback receiveCallback;

    private final EndpointParameters parameters = new EndpointParameters();
    private final RequestParameters sendParameters = new RequestParameters();
    private final RequestParameters tagReceiveParameters = new RequestParameters();
    private final RequestParameters streamReceiveParameters = new RequestParameters();
    private final RequestParameters emptyParameters = new RequestParameters();

    private boolean errorState = false;

    InfinileapEndpoint(final Context context) throws ControlException {
        worker = new InfinileapWorker(context, new WorkerParameters());
    }

    InfinileapEndpoint(final Context context, final ConnectionRequest connectionRequest) throws ControlException {
        remoteAddress = connectionRequest.getClientAddress();
        worker = new InfinileapWorker(context, new WorkerParameters());
        endpoint = worker.getWorker().createEndpoint(
                parameters.setConnectionRequest(connectionRequest)
                .setErrorHandler((userData, endpoint, status) -> {
                    LOGGER.error("A UCX error occurred (Status: [{}])!", status);
                    handleError();
        }));

        LOGGER.info("Endpoint created");
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress) throws IOException {
        try {
            this.remoteAddress = remoteAddress;
            endpoint = worker.getWorker().createEndpoint(
                    parameters.setRemoteAddress(remoteAddress)
                    .setErrorHandler((userData, endpoint, status) -> {
                        LOGGER.error("A UCX error occurred (Status: [{}])!", status);
                        handleError();
                    }));
        } catch (ControlException e) {
            throw new IOException(e);
        }

        LOGGER.info("Endpoint created");
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var status = endpoint.sendTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), Tag.of(tag), useCallback ? sendParameters : emptyParameters);
        if (Status.isStatus(status)) {
            if (useCallback && Status.is(status, Status.OK)) {
                sendCallback.onMessageSent();
            }

            return true;
        } else if (blocking) {
            waitRequest(status);
            return true;
        }

        return false;
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var status = worker.getWorker().receiveTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), Tag.of(tag), TAG_MASK_FULL, useCallback ? tagReceiveParameters : emptyParameters);
        if (Status.isStatus(status)) {
            if (useCallback && Status.is(status, Status.OK)) {
                receiveCallback.onMessageReceived(tag);
            }

            return true;
        } else if (blocking) {
            waitRequest(status);
            return true;
        }

        return false;
    }

    @Override
    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var status = endpoint.sendStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), size, useCallback ? sendParameters : emptyParameters);
        if (Status.isStatus(status)) {
            if (useCallback && Status.is(status, Status.OK)) {
                sendCallback.onMessageSent();
            }
        } else if (blocking) {
            waitRequest(status);
        }
    }

    @Override
    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var receiveSize = new NativeLong();
        final var status = endpoint.receiveStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global()), size, receiveSize, useCallback ? streamReceiveParameters : emptyParameters);
        if (Status.isStatus(status)) {
            if (useCallback && Status.is(status, Status.OK)) {
                receiveCallback.onMessageReceived(0);
            }
        } else if (blocking) {
            waitRequest(status);
        }
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        this.sendCallback = sendCallback;
        sendParameters.setSendCallback(
            (request, status, data) -> {
                if (status == Status.OK) {
                    LOGGER.debug("Infinileap SendCallback called (Status: [{}])", status);
                    sendCallback.onMessageSent();
                } else {
                    LOGGER.error("Failed to send a message (Status: [{}])!", status);
                    handleError();
                }

                ucp_request_free(request);
            });
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        this.receiveCallback = receiveCallback;
        tagReceiveParameters.setReceiveCallback(
            (request, status, tagInfo, data) -> {
                if (status == Status.OK) {
                    final var tag = tagInfo.get(OfLong.JAVA_LONG, 0);
                    final var size = tagInfo.get(OfLong.JAVA_LONG, Long.BYTES);
                    LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}], Tag: [0x{}])", status, size, Long.toHexString(tag));
                    receiveCallback.onMessageReceived(tag);
                } else {
                    LOGGER.error("Failed to receive a message (Status: [{}])!", status);
                    handleError();
                }

                ucp_request_free(request);
            });

        streamReceiveParameters.setReceiveCallback(
                (request, status, length, data) -> {
                    if (status == Status.OK) {
                        LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}])", status, length);
                        receiveCallback.onMessageReceived(0);
                    } else {
                        LOGGER.error("Failed to receive a message (Status: [{}])!", status);
                        handleError();
                    }

                    ucp_request_free(request);
                });
    }

    @Override
    public boolean getErrorState() {
        return errorState;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
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

    private void handleError() {
        errorState = true;
    }

    private void waitRequest(long requestHandle) {
        long status = ucp_request_check_status(requestHandle);
        while (Status.is(status, Status.IN_PROGRESS)) {
            worker.getWorker().progress();
            status = ucp_request_check_status(requestHandle);
        }
    }
}
