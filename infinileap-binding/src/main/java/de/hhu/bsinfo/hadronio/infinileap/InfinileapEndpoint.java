package de.hhu.bsinfo.hadronio.infinileap;

import static org.openucx.Communication.ucp_request_check_status;

import de.hhu.bsinfo.hadronio.binding.UcxCallback;
import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxException;
import de.hhu.bsinfo.hadronio.binding.UcxReceiveCallback;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.EndpointParameters;
import de.hhu.bsinfo.infinileap.binding.RequestParameters;
import de.hhu.bsinfo.infinileap.binding.Status;
import de.hhu.bsinfo.infinileap.binding.Tag;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.primitive.NativeLong;
import java.io.IOException;
import java.net.InetSocketAddress;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout.OfLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfinileapEndpoint implements UcxEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinileapEndpoint.class);

    private final InfinileapWorker worker;
    private Endpoint endpoint;
    private final RequestParameters sendParameters = new RequestParameters();
    private final RequestParameters receiveParameters = new RequestParameters();
    private final RequestParameters emptyParameters = new RequestParameters();

    InfinileapEndpoint(final Context context) throws ControlException {
        worker = new InfinileapWorker(context, new WorkerParameters());
    }

    InfinileapEndpoint(final Context context, final ConnectionRequest connectionRequest) throws ControlException {
        worker = new InfinileapWorker(context, new WorkerParameters());
        endpoint = worker.getWorker().createEndpoint(new EndpointParameters().setConnectionRequest(connectionRequest));
        LOGGER.info("Endpoint created");
    }

    @Override
    public void connect(final InetSocketAddress remoteAddress) throws IOException {
        try {
            endpoint = worker.getWorker().createEndpoint(new EndpointParameters().setRemoteAddress(remoteAddress));
        } catch (ControlException e) {
            throw new IOException(e);
        }

        LOGGER.info("Endpoint created");
    }

    @Override
    public boolean sendTaggedMessage(final long address, final long size, final long tag, final boolean useCallback, final boolean blocking) {
        final var status = endpoint.sendTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, ResourceScope.globalScope()), Tag.of(tag), useCallback ? sendParameters : emptyParameters);
        return checkStatus(status, blocking);
    }

    @Override
    public boolean receiveTaggedMessage(final long address, final long size, final long tag, final long tagMask, final boolean useCallback, final boolean blocking) {
        final var status = worker.getWorker().receiveTagged(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, ResourceScope.globalScope()), Tag.of(tag), useCallback ? receiveParameters : emptyParameters);
        return checkStatus(status, blocking);
    }

    @Override
    public void sendStream(final long address, final long size, final UcxCallback callback) {
        final var retStatus = endpoint.sendStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, ResourceScope.globalScope()), size, new RequestParameters().setSendCallback(
            (request, status, data) -> {
                if (status == Status.OK) {
                    callback.onSuccess();
                } else {
                    throw new UcxException("Failed to send data via streaming (Status: [" + status + "])!");
                }
            }));

        if (checkStatus(retStatus, false)) {
            callback.onSuccess();
        }
    }

    @Override
    public void receiveStream(final long address, final long size, final UcxCallback callback) {
        final var receiveSize = new NativeLong();
        final var retStatus = endpoint.receiveStream(MemorySegment.ofAddress(MemoryAddress.ofLong(address), size, ResourceScope.globalScope()), size, receiveSize, new RequestParameters().setReceiveCallback(
            (request, status, tagInfo, data) -> {
                if (status == Status.OK) {
                    callback.onSuccess();
                } else {
                    throw new UcxException("Failed to receive data via streaming (Status: [" + status + "])!");
                }
            }));

        if (checkStatus(retStatus, false)) {
            callback.onSuccess();
        }
    }

    @Override
    public void setSendCallback(final UcxCallback sendCallback) {
        sendParameters.setSendCallback(
            (request, status, data) -> {
                if (status == Status.OK) {
                    LOGGER.debug("Infinileap SendCallback called (Status: [{}])", status);
                    sendCallback.onSuccess();
                } else {
                    throw new UcxException("Failed to send a message (Status: [" + status + "])");
                }
            });
    }

    @Override
    public void setReceiveCallback(final UcxReceiveCallback receiveCallback) {
        receiveParameters.setReceiveCallback(
            (request, status, tagInfo, data) -> {
                if (status == Status.OK) {
                    final var tag = tagInfo.get(OfLong.JAVA_LONG, 0);
                    final var size = tagInfo.get(OfLong.JAVA_LONG, Long.BYTES);
                    LOGGER.debug("Infinileap ReceiveCallback called (Status: [{}], Size: [{}], Tag: [0x{}])", status, size, Long.toHexString(tag));
                    receiveCallback.onMessageReceived(tag);
                } else {
                    throw new UcxException("Failed to receive a message (Status: [" + status + "])");
                }
            });
    }

    @Override
    public UcxWorker getWorker() {
        return worker;
    }

    @Override
    public void close() {
        LOGGER.info("Closing endpoint");
    }

    private boolean checkStatus(long status, boolean blocking) {
        if (Status.is(status, Status.OK)) {
            return true;
        }

        while (blocking && !Status.is(ucp_request_check_status(status), Status.OK)) {
            worker.getWorker().progress();
        }

        return Status.is(status, Status.OK);
    }
}
