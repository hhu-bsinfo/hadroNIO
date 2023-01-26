package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxSendCallback;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.generated.DebugConfig;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.primitive.NativeLong;
import java.io.IOException;
import java.lang.foreign.MemorySession;
import java.net.InetSocketAddress;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfLong;

import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.openucx.Communication.*;

class InfinileapEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapEndpoint.class);

    private static final Tag TAG_MASK_FULL = Tag.of(TagUtil.TAG_MASK_FULL);
    private static final NativeLong STREAM_RECEIVE_SIZE = new NativeLong();

    private Endpoint endpoint;
    private final InfinileapWorker worker;
    private InetSocketAddress remoteAddress;

    private UcxSendCallback sendCallback;
    private UcxReceiveCallback receiveCallback;

    private final EndpointParameters parameters = new EndpointParameters();
    private final RequestParameters sendParameters = new RequestParameters();
    private final RequestParameters tagReceiveParameters = new RequestParameters();
    private final RequestParameters streamReceiveParameters = new RequestParameters();
    private final RequestParameters emptyParameters = new RequestParameters();

    private final Long2ObjectHashMap<MemorySegment> receiveSegmentCache = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Tag> tagCache = new Long2ObjectHashMap<>();

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
        final var segment = MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global());
        final var status = endpoint.sendTagged(segment, getCachedTag(tag), useCallback ? sendParameters : emptyParameters);
        return waitSendStatus(status, useCallback, blocking);
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var segment = getCachedSegment(address, size);
        final var status = worker.getWorker().receiveTagged(segment, getCachedTag(tag), TAG_MASK_FULL, useCallback ? tagReceiveParameters : emptyParameters);
        return waitReceiveStatus(status, tag, useCallback, blocking);
    }

    @Override
    public void sendStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var segment = MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global());
        final var status = endpoint.sendStream(segment, size, useCallback ? sendParameters : emptyParameters);
        waitSendStatus(status, useCallback, blocking);
    }

    @Override
    public void receiveStream(final long address, final long size, final boolean useCallback, final boolean blocking) {
        final var segment = MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global());
        final var status = endpoint.receiveStream(segment, size, STREAM_RECEIVE_SIZE, useCallback ? streamReceiveParameters : emptyParameters);
        waitReceiveStatus(status, 0, useCallback, blocking);
    }

    @Override
    public void setSendCallback(final UcxSendCallback sendCallback) {
        this.sendCallback = sendCallback;
        sendParameters.setSendCallback(
            (request, status, data) -> {
                if (status == Status.OK) {
                    if (DebugConfig.DEBUG) LOGGER.debug("Infinileap SendCallback called (Status: [{}])", status);
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
                    if (DebugConfig.DEBUG) LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}], Tag: [0x{}])", status, size, Long.toHexString(tag));
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
                        if (DebugConfig.DEBUG) LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}])", status, length);
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

    private boolean waitSendStatus(final long status, final boolean useCallback, final boolean blocking) {
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

    private boolean waitReceiveStatus(final long status, final long tag, final boolean useCallback, final boolean blocking) {
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

    private void waitRequest(final long requestHandle) {
        long status = ucp_request_check_status(requestHandle);
        while (Status.is(status, Status.IN_PROGRESS)) {
            worker.getWorker().progress();
            status = ucp_request_check_status(requestHandle);
        }
    }

    private Tag getCachedTag(final long tag) {
        var tagObject = tagCache.get(tag);
        if (tagObject == null) {
            tagObject = Tag.of(tag);
            tagCache.put(tag, tagObject);
        }

        return tagObject;
    }

    private MemorySegment getCachedSegment(final long address, final long size) {
        var addressObject = receiveSegmentCache.get(address);
        if (addressObject == null) {
            addressObject = MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, MemorySession.global());
            receiveSegmentCache.put(address, addressObject);
        }

        return addressObject;
    }
}
