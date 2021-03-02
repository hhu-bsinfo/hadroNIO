package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.agrona.concurrent.ringbuffer.RingBuffer.INSUFFICIENT_CAPACITY;

public class UcxSocketChannel extends SocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxSocketChannel.class);
    private static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    private static final int SEND_BUFFER_MIN_REMAINING = 16;
    private static final int HEADER_LENGTH = Integer.BYTES * 2;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;
    private final RingBuffer sendBuffer;
    private final RingBuffer receiveBuffer;
    private final int receiveSliceLength;
    private final AtomicInteger readableMessages = new AtomicInteger();

    private UcpEndpoint endpoint;
    private UcxCallback sendCallback;
    private UcxCallback receiveCallback;

    private boolean connected = false;
    private boolean inputClosed = false;
    private boolean outputClosed = false;
    private int readyOps = 0;

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, int sendBufferLength, int receiveBufferLength, int receiveSliceLength) {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        this.receiveSliceLength = receiveSliceLength;
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpConnectionRequest connectionRequest, int sendBufferLength, int receiveBufferLength, int receiveSliceLength) throws IOException {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        this.receiveSliceLength = receiveSliceLength;

        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        connected = establishConnection();
    }

    @Override
    public SocketChannel bind(SocketAddress socketAddress) throws IOException {
        LOGGER.warn("Trying to bind socket channel to [{}], but binding is not supported", socketAddress);

        return this;
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        LOGGER.warn("Trying to set option [{}], which is not supported", socketOption.name());

        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
        LOGGER.warn("Trying to get option [{}], which is not supported", socketOption.name());

        return null;
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be readable");

        inputClosed = true;
        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        LOGGER.info("Closing connection for input -> This socket channel will no longer be writeable");

        outputClosed = true;
        return this;
    }

    @Override
    public Socket socket() {
        LOGGER.error("Direct socket access is not supported");

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isConnectionPending() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public boolean connect(SocketAddress remoteAddress) throws IOException {
        LOGGER.info("Connecting to [{}]", remoteAddress);

        return (connected = connectTo(remoteAddress));
    }

    private boolean connectTo(SocketAddress remoteAddress) {
        UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        return establishConnection();
    }

    private boolean establishConnection() {
        ByteBuffer sendBuffer = ByteBuffer.allocateDirect(8);
        ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(8);

        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.rewind();

        ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer);

        LOGGER.info("Exchanging small message to establish connection");

        endpoint.sendTaggedNonBlocking(sendBuffer, connectionCallback);
        worker.recvTaggedNonBlocking(receiveBuffer, connectionCallback);

        return connected;
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (connected) {
            readyOps &= ~SelectionKey.OP_CONNECT;
            return true;
        }

        if (isBlocking()) {
            LOGGER.info("Waiting for connection to be established");

            while (!connected) {
                try {
                    while (worker.progress() == 0) {
                        worker.waitForEvents();
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to progress worker while waiting for connection to be established", e);
                }
            }
        }

        readyOps &= ~SelectionKey.OP_CONNECT;

        return connected;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        LOGGER.warn("Trying to get remote address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        if (inputClosed) {
            return -1;
        }

        AtomicBoolean messageCompleted = new AtomicBoolean(false);
        AtomicInteger readBytes = new AtomicInteger();

        int read = receiveBuffer.read((msgTypeId, directBuffer, index, bufferLength) -> {
            int readable = readableMessages.decrementAndGet();
            int messageLength = directBuffer.getInt(index);
            int offset = directBuffer.getInt(index + Integer.BYTES);
            LOGGER.debug("Message type id: [{}], Index: [{}], Offset: [{}], Buffer Length: [{}], Message Length: [{}], Readable messages: [{}]", msgTypeId, index, offset, bufferLength, messageLength, readable);

            int length = Math.min(buffer.remaining(), messageLength - offset);
            directBuffer.getBytes(index + HEADER_LENGTH + offset, buffer, length);

            if (length == messageLength) {
                messageCompleted.set(true);
            } else {
                directBuffer.putInt(index + Integer.BYTES, offset + length);
            }

            readBytes.set(length);
        }, 1);

        if (messageCompleted.get()) {
            receiveBuffer.commitRead(read);
        }

        return readBytes.get();
    }

    @Override
    public long read(ByteBuffer[] buffers, int i, int i1) throws IOException {
        if (inputClosed) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        if (outputClosed) {
            return -1;
        }

        int length = Math.min(buffer.remaining() + HEADER_LENGTH, sendBuffer.capacity() - sendBuffer.size());
        if (length <= HEADER_LENGTH) {
            LOGGER.error("Unable to claim space in the send buffer (Error: [{}])", INSUFFICIENT_CAPACITY);
            return 0;
        }

        int index = sendBuffer.tryClaim(length);

        if (index <= 0) {
            LOGGER.error("Unable to claim space in the send buffer (Error: [{}])", index);
            return 0;
        }

        // Put message length
        sendBuffer.buffer().putInt(index, length - HEADER_LENGTH);
        // Put number of read bytes (initially 0)
        sendBuffer.buffer().putInt(index + Integer.BYTES, 0);
        // Put message
        sendBuffer.buffer().putBytes(index + HEADER_LENGTH, buffer, buffer.position(), length - HEADER_LENGTH);

        ByteBuffer directBuffer = sendBuffer.buffer().byteBuffer().duplicate();
        directBuffer.limit(index + length);
        directBuffer.position(index);

        sendBuffer.commitWrite(index);
        endpoint.sendTaggedNonBlocking(directBuffer, sendCallback);

        return length;
    }

    @Override
    public long write(ByteBuffer[] buffers, int i, int i1) throws IOException {
        if (outputClosed) {
            return -1;
        }

        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        LOGGER.warn("Trying to get local address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing socket channel");
        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        if (connected && !outputClosed && sendBuffer.capacity() - sendBuffer.size() > SEND_BUFFER_MIN_REMAINING) {
            readyOps |= SelectionKey.OP_WRITE;
        } else {
            readyOps &= ~SelectionKey.OP_WRITE;
        }

        if (readableMessages.get() > 0 && !inputClosed) {
            readyOps |= SelectionKey.OP_READ;
        } else {
            readyOps &= ~SelectionKey.OP_READ;
        }

        return readyOps;
    }

    @Override
    public void select() throws IOException {
        if (connected) {
            fillReceiveBuffer();
        }

        try {
            worker.progress();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void fillReceiveBuffer() {
        int index = receiveBuffer.tryClaim(receiveSliceLength);

        while (index > 0) {
            LOGGER.debug("Claimed part of the receive buffer (Index: [{}], Length: [{}])", index, receiveSliceLength);

            ByteBuffer directBuffer = receiveBuffer.buffer().byteBuffer().duplicate();
            directBuffer.limit(index + receiveSliceLength);
            directBuffer.position(index);

            receiveBuffer.commitWrite(index);
            worker.recvTaggedNonBlocking(directBuffer, receiveCallback);

            index = receiveBuffer.tryClaim(receiveSliceLength);
        }
    }

    private static final class ConnectionCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCallback.class);

        private final UcxSocketChannel socket;
        private final ByteBuffer receiveBuffer;
        private final AtomicInteger successCounter = new AtomicInteger(0);

        private ConnectionCallback(UcxSocketChannel socket, ByteBuffer receiveBuffer) {
            this.socket = socket;
            this.receiveBuffer = receiveBuffer;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            if (request.isCompleted()) {
                int count = successCounter.incrementAndGet();

                LOGGER.info("Connection callback has been called with a successfully completed request ([{}/2])", successCounter.get());

                if (count == 2) {
                    long magic = receiveBuffer.getLong();

                    if (magic != CONNECTION_MAGIC_NUMBER) {
                        LOGGER.error("Connection callback has been called, but magic number is wrong! Expected: [{}], Received: [{}] -> Discarding connection", Long.toHexString(CONNECTION_MAGIC_NUMBER), Long.toHexString(magic));
                        return;
                    }

                    socket.sendCallback = new SendCallback(socket.sendBuffer);
                    socket.receiveCallback = new ReceiveCallback(socket.readableMessages);

                    socket.fillReceiveBuffer();

                    socket.connected = true;
                    socket.readyOps |= SelectionKey.OP_CONNECT;

                    successCounter.set(0);
                }
            }
        }
    }

    private static final class SendCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

        private final RingBuffer sendBuffer;

        private SendCallback(RingBuffer sendBuffer) {
            this.sendBuffer = sendBuffer;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            LOGGER.debug("SendCallback called (Completed: [{}])", request.isCompleted());

            int read = sendBuffer.read((msgTypeId, buffer, index, length) -> {
                LOGGER.debug("Message type id: [{}], Index: [{}], Length: [{}]", msgTypeId, index, length);
            }, 1);

            sendBuffer.commitRead(read);
        }
    }

    private static final class ReceiveCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

        private final AtomicInteger readableMessages;

        private ReceiveCallback(AtomicInteger readableMessages) {
            this.readableMessages = readableMessages;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            int readable = readableMessages.incrementAndGet();
            LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}], Readable messages: [{}])", request.isCompleted(), request.getRecvSize(), readable);
        }
    }
}
