package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.ResourceHandler;
import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.QueuedPipe;
import org.agrona.hints.ThreadHints;
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
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.agrona.concurrent.ringbuffer.RingBuffer.INSUFFICIENT_CAPACITY;

public class UcxSocketChannel extends SocketChannel implements UcxSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxSocketChannel.class);

    private static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    private static final long CONNECTION_TAG = 0;
    private static final long MESSAGE_TAG = 1;
    private static final long TAG_MASK = 0xffffffffffffffffL;

    private static final int HEADER_LENGTH = Integer.BYTES * 2;
    private static final int HEADER_OFFSET_MESSAGE_LENGTH = 0;
    private static final int HEADER_OFFSET_READ_BYTES = Integer.BYTES;
    private static final int HEADER_OFFSET_MESSAGE_DATA = Integer.BYTES * 2;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private UcxCallback sendCallback;
    private UcxCallback receiveCallback;

    private final RingBuffer sendBuffer;
    private final RingBuffer receiveBuffer;
    private final QueuedPipe<Integer> receiveIndexQueue;
    private final int receiveSliceLength;

    private final AtomicInteger readableMessages = new AtomicInteger();
    private final Lock readLock = new ReentrantLock();
    private final Lock writeLock = new ReentrantLock();

    private boolean connectable = false;
    private boolean connectionPending = false;
    private boolean connectionFailed = false;
    private boolean connected = false;
    private boolean inputClosed = false;
    private boolean outputClosed = false;
    private boolean channelClosed = false;

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, int sendBufferLength, int receiveBufferLength, int receiveSliceLength) {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        receiveIndexQueue = new ManyToManyConcurrentArrayQueue<>(receiveBufferLength / receiveSliceLength);
        this.receiveSliceLength = receiveSliceLength;
    }

    protected UcxSocketChannel(SelectorProvider provider, UcpContext context, UcpConnectionRequest connectionRequest, int sendBufferLength, int receiveBufferLength, int receiveSliceLength) throws IOException {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        receiveIndexQueue = new ManyToManyConcurrentArrayQueue<>(receiveBufferLength / receiveSliceLength + 1);
        this.receiveSliceLength = receiveSliceLength;

        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        connectionPending = true;
        establishConnection();

        if (isBlocking()) {
            do {
                UcxSelectableChannel.pollWorkerBlocking(worker);

                if (connectionFailed) {
                    throw new IOException("Failed to connect!");
                }
            } while (!connected);
        }

        if (connected) {
            connectable = false;
            connectionPending = false;
        }
    }

    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (local != null) {
            LOGGER.warn("Trying to bind socket channel to [{}], but binding is not supported", local);
        }

        return this;
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> socketOption, T t) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public <T> T getOption(SocketOption<T> socketOption) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        if (!connected) {
            throw new NotYetConnectedException();
        }
        
        if (channelClosed) {
            throw new ClosedChannelException();
        }
        
        LOGGER.info("Closing connection for input -> This socket channel will no longer be readable");

        inputClosed = true;
        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        if (!connected) {
            throw new NotYetConnectedException();
        }

        if (channelClosed) {
            throw new ClosedChannelException();
        }
        
        LOGGER.info("Closing connection for input -> This socket channel will no longer be writeable");

        outputClosed = true;
        return this;
    }

    @Override
    public Socket socket() {
        throw new UnsupportedOperationException("Direct socket access is not supported!");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean isConnectionPending() {
        return connectionPending;
    }

    @Override
    public boolean connect(SocketAddress remoteAddress) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (connected) {
            throw new AlreadyConnectedException();
        }

        if (connectionPending) {
            throw new ConnectionPendingException();
        }

        LOGGER.info("Connecting to [{}]", remoteAddress);

        connectionPending = true;
        connectTo(remoteAddress);

        if (isBlocking()) {
            do {
                UcxSelectableChannel.pollWorkerBlocking(worker);

                if (connectionFailed) {
                    throw new IOException("Failed to connect!");
                }
            } while (!connected);
        }

        if (connected) {
            connectable = false;
            connectionPending = false;
            return true;
        }

        return false;
    }

    private void connectTo(SocketAddress remoteAddress) {
        UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        establishConnection();
    }

    private void establishConnection() {
        ByteBuffer sendBuffer = ByteBuffer.allocateDirect(8);
        ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(8);

        sendBuffer.putLong(CONNECTION_MAGIC_NUMBER);
        sendBuffer.rewind();

        ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer);

        LOGGER.info("Exchanging small message to establish connection");

        endpoint.sendTaggedNonBlocking(sendBuffer, CONNECTION_TAG, connectionCallback);
        worker.recvTaggedNonBlocking(receiveBuffer, CONNECTION_TAG, TAG_MASK, connectionCallback);
    }

    @Override
    public boolean finishConnect() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connectionPending) {
            throw new NoConnectionPendingException();
        }

        if (connectionFailed) {
            close();
            throw new IOException("Failed to connect!");
        }

        if (connected) {
            connectable = false;
            return true;
        } else if (!isBlocking()) {
            return false;
        } else {
            LOGGER.info("Waiting for connection to be established");


            do {
                UcxSelectableChannel.pollWorkerBlocking(worker);

                if (connectionFailed) {
                    throw new IOException("Failed to connect!");
                }
            } while (!connected);

            connectable = false;
            return true;
        }
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        LOGGER.warn("Trying to get remote address, which is not supported");

        return new InetSocketAddress(0);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        if (inputClosed) {
            return -1;
        }

        readLock.lock();

        if (isBlocking()) {
            while (readableMessages.get() <= 0) {
                fillReceiveBuffer();
                UcxSelectableChannel.pollWorkerNonBlocking(worker);
                ThreadHints.onSpinWait();
            }
        }

        if (readableMessages.get() <= 0) {
            return 0;
        }

        AtomicBoolean padding = new AtomicBoolean(true);
        AtomicBoolean messageCompleted = new AtomicBoolean(false);
        AtomicInteger readBytes = new AtomicInteger();
        int readFromBuffer = 0;

        do {
            readFromBuffer = receiveBuffer.read((msgTypeId, directBuffer, index, bufferLength) -> {
                // Get message length
                int messageLength = directBuffer.getInt(index + HEADER_OFFSET_MESSAGE_LENGTH);
                // Get message offset
                int offset = directBuffer.getInt(index + HEADER_OFFSET_READ_BYTES);
                LOGGER.debug("Message type id: [{}], Index: [{}], Buffer Length: [{}], Message Length: [{}], Offset: [{}]", msgTypeId, index, bufferLength, messageLength, offset);

                int length = Math.min(buffer.remaining(), messageLength - offset);
                // Get message data
                directBuffer.getBytes(index + HEADER_OFFSET_MESSAGE_DATA + offset, buffer, length);
                // Put updated message offset
                directBuffer.putInt(index + HEADER_OFFSET_READ_BYTES, offset + length);

                if (length == messageLength) {
                    messageCompleted.set(true);
                }

                padding.set(false);
                readBytes.set(length);
            }, 1);

            if (padding.get()) {
                LOGGER.debug("Read [{}] padding bytes from buffer", readFromBuffer);
                receiveBuffer.commitRead(readFromBuffer);
            }
        } while (padding.get());

        if (messageCompleted.get()) {
            int readable = readableMessages.decrementAndGet();
            LOGGER.debug("Readable messages left: [{}]", readable);
            receiveBuffer.commitRead(readFromBuffer);
        }

        readLock.unlock();
        return readBytes.get();
    }

    @Override
    public long read(ByteBuffer[] buffers, int offset, int length) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        if (inputClosed) {
            return -1;
        }

        readLock.lock();

        int readTotal = 0;

        for (int i = 0; i < length; i++) {
            int read;

            try {
                read = read(buffers[offset + i]);
            } catch (ClosedChannelException e) {
                throw new AsynchronousCloseException();
            }

            if(buffers[offset + i].remaining() > 0) {
                break;
            }

            readTotal += read;
        }

        readLock.unlock();

        return readTotal;
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        if (outputClosed) {
            return -1;
        }

        writeLock.lock();

        int written = 0;

        do {
            int length = Math.min(buffer.remaining(), sendBuffer.maxMessageLength());
            int index;

            if (length > 0) {
                index = sendBuffer.tryClaim(length);
            } else {
                index = INSUFFICIENT_CAPACITY;
            }

            if (index < 0) {
                LOGGER.warn("Unable to claim space in the send buffer (Error: [{}])", index);

                if (isBlocking()) {
                    UcxSelectableChannel.pollWorkerBlocking(worker);
                    continue;
                }
            }

            sendBuffer.buffer().putBytes(index, buffer, buffer.position(), length);
            // Advance position manually, since AtomicBuffer.putBytes() does not do it
            buffer.position(buffer.position() + length);

            sendBuffer.commitWrite(index);
            UcpRequest request = endpoint.sendTaggedNonBlocking(sendBuffer.memoryAddress() + index, length, MESSAGE_TAG, sendCallback);

            if (isBlocking()) {
                try {
                    worker.progressRequest(request);
                } catch (Exception e) {
                    throw new IOException("Failed to progress worker while sending a message!", e);
                }
            }

            written += length - HEADER_LENGTH;
        } while (isBlocking() && buffer.hasRemaining());

        writeLock.unlock();

        return written;
    }

    @Override
    public long write(ByteBuffer[] buffers, int offset, int length) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        if (outputClosed) {
            return -1;
        }

        writeLock.lock();

        int writtenTotal = 0;

        for (int i = 0; i < length; i++) {
            int written;

            try {
                written = write(buffers[offset + i]);
            } catch (ClosedChannelException e) {
                throw new AsynchronousCloseException();
            }

            if(buffers[offset + i].remaining() > 0) {
                break;
            }

            writtenTotal += written;
        }

        writeLock.unlock();

        return writtenTotal;
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        LOGGER.warn("Trying to get local address, which is not supported");
        return new InetSocketAddress(0);
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing socket channel");
        channelClosed = true;
        resourceHandler.close();
    }

    @Override
    protected void implConfigureBlocking(boolean blocking) throws IOException {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        int readyOps = 0;

        if (connectable) {
            readyOps |= SelectionKey.OP_CONNECT;
        }

        if (connected && !outputClosed && sendBuffer.maxMessageLength() > 0) {
            readyOps |= SelectionKey.OP_WRITE;
        }

        if (!inputClosed && readableMessages.get() > 0) {
            readyOps |= SelectionKey.OP_READ;
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
        int length = Math.min(receiveSliceLength, receiveBuffer.maxMessageLength());
        int index;

        if (length > 0) {
            index = receiveBuffer.tryClaim(length);
        } else {
            index = INSUFFICIENT_CAPACITY;
        }

        while (index >= 0) {
            LOGGER.debug("Claimed part of the receive buffer (Index: [{}], Length: [{}])", index, length);

            receiveBuffer.commitWrite(index);
            receiveIndexQueue.add(index);
            worker.recvTaggedNonBlocking(receiveBuffer.memoryAddress() + index + HEADER_LENGTH, receiveSliceLength - HEADER_LENGTH, MESSAGE_TAG, TAG_MASK, receiveCallback);

            length = Math.min(receiveSliceLength, receiveBuffer.maxMessageLength());
            if (length > 0) {
                index = receiveBuffer.tryClaim(length);
            } else {
                index = INSUFFICIENT_CAPACITY;
            }
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

                LOGGER.info("Connection callback has been called with a successfully completed request ([{}/2])", count);

                if (count == 2) {
                    long magic = receiveBuffer.getLong();

                    if (magic != CONNECTION_MAGIC_NUMBER) {
                        LOGGER.error("Connection callback has been called, but magic number is wrong! Expected: [{}], Received: [{}] -> Discarding connection", Long.toHexString(CONNECTION_MAGIC_NUMBER), Long.toHexString(magic));

                        socket.connected = false;
                        socket.connectionFailed = true;
                        socket.connectable = true;
                        return;
                    }

                    socket.sendCallback = new SendCallback(socket, socket.sendBuffer);
                    socket.receiveCallback = new ReceiveCallback(socket, socket.receiveBuffer, socket.receiveIndexQueue, socket.readableMessages);

                    socket.fillReceiveBuffer();

                    socket.connected = true;
                    socket.connectable = true;

                    successCounter.set(0);
                }
            }
        }

        @Override
        public void onError(int ucsStatus, String errorMessage) {
            LOGGER.error("Failed to establish connection! Status: [{}], Error: [{}]", ucsStatus, errorMessage);

            socket.connected = false;
            socket.connectionFailed = true;
            socket.connectable = true;
        }
    }

    private static final class SendCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(SendCallback.class);

        private final SocketChannel socket;
        private final RingBuffer sendBuffer;

        private SendCallback(SocketChannel socket, RingBuffer sendBuffer) {
            this.socket = socket;
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

        @Override
        public void onError(int ucsStatus, String errorMessage) {
            LOGGER.error("Failed to send a message! Status: [{}], Error: [{}]", ucsStatus, errorMessage);

            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close socket channel", e);
            }
        }
    }

    private static final class ReceiveCallback extends UcxCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveCallback.class);

        private final SocketChannel socket;
        private final RingBuffer receiveBuffer;
        private final QueuedPipe<Integer> receiveIndexQueue;
        private final AtomicInteger readableMessages;

        private ReceiveCallback(SocketChannel socket, RingBuffer receiverBuffer, QueuedPipe<Integer> receiveIndexQueue, AtomicInteger readableMessages) {
            this.socket = socket;
            this.receiveBuffer = receiverBuffer;
            this.receiveIndexQueue = receiveIndexQueue;
            this.readableMessages = readableMessages;
        }

        @Override
        public void onSuccess(UcpRequest request) {
            Integer index = receiveIndexQueue.poll();
            if (index == null) {
                throw new IllegalStateException("Receive index queue is empty!");
            }

            LOGGER.debug("ReceiveCallback called (Completed: [{}], Size: [{}], Index: [{}])", request.isCompleted(), request.getRecvSize(), index);

            // Put message length
            receiveBuffer.buffer().putInt(index + HEADER_OFFSET_MESSAGE_LENGTH, (int) request.getRecvSize());
            // Put number of read bytes (initially 0)
            receiveBuffer.buffer().putInt(index + HEADER_OFFSET_READ_BYTES, 0);

            int readable = readableMessages.incrementAndGet();
            LOGGER.debug("Readable messages left: [{}]", readable);
        }

        @Override
        public void onError(int ucsStatus, String errorMessage) {
            LOGGER.error("Failed to receive a message! Status: [{}], Error: [{}]", ucsStatus, errorMessage);

            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close socket channel", e);
            }
        }
    }
}
