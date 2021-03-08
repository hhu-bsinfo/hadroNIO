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

    static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    static final long CONNECTION_TAG = 0;
    static final long MESSAGE_TAG = 1;
    static final long TAG_MASK = 0xffffffffffffffffL;

    static final int HEADER_LENGTH = Integer.BYTES * 2;
    static final int HEADER_OFFSET_MESSAGE_LENGTH = 0;
    static final int HEADER_OFFSET_READ_BYTES = Integer.BYTES;
    static final int HEADER_OFFSET_MESSAGE_DATA = Integer.BYTES * 2;

    private final ResourceHandler resourceHandler = new ResourceHandler();
    private final UcpWorker worker;
    private UcpEndpoint endpoint;
    private UcxCallback sendCallback;
    private UcxCallback receiveCallback;

    private final RingBuffer sendBuffer;
    private final RingBuffer receiveBuffer;
    private final int bufferSliceLength;

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

    protected UcxSocketChannel(final SelectorProvider provider, final UcpContext context, final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength) {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        resourceHandler.addResource(worker);
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        this.bufferSliceLength = bufferSliceLength;
    }

    protected UcxSocketChannel(final SelectorProvider provider, final UcpContext context, final UcpConnectionRequest connectionRequest, final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength) throws IOException {
        super(provider);

        worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        endpoint = worker.newEndpoint(new UcpEndpointParams().setConnectionRequest(connectionRequest).setPeerErrorHandlingMode());
        sendBuffer = new RingBuffer(sendBufferLength);
        receiveBuffer = new RingBuffer(receiveBufferLength);
        this.bufferSliceLength = bufferSliceLength;

        resourceHandler.addResource(worker);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);

        connectionPending = true;
        establishConnection();

        if (isBlocking()) {
            if (!finishConnect()) {
                throw new IOException("Failed to connect socket channel!");
            }
        }

        if (connected) {
            connectable = false;
            connectionPending = false;
        }
    }

    @Override
    public SocketChannel bind(final SocketAddress local) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (local != null) {
            LOGGER.warn("Trying to bind socket channel to [{}], but binding is not supported", local);
        }

        return this;
    }

    @Override
    public <T> SocketChannel setOption(final SocketOption<T> socketOption, T t) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new UnsupportedOperationException("Trying to set unsupported option " + socketOption.name() + "!");
    }

    @Override
    public <T> T getOption(final SocketOption<T> socketOption) throws IOException {
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
    public boolean connect(final SocketAddress remoteAddress) throws IOException {
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
            if (!finishConnect()) {
                throw new IOException("Failed to connect socket channel!");
            }
        }

        if (connected) {
            connectable = false;
            connectionPending = false;
            return true;
        }

        return false;
    }

    private void connectTo(final SocketAddress remoteAddress) {
        final UcpEndpointParams endpointParams = new UcpEndpointParams().setSocketAddress((InetSocketAddress) remoteAddress).setPeerErrorHandlingMode();
        endpoint = worker.newEndpoint(endpointParams);
        resourceHandler.addResource(endpoint);

        LOGGER.info("Endpoint created: [{}]", endpoint);
        establishConnection();
    }

    private void establishConnection() {
        final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(8);
        final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(8);

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
            throw new IOException("Failed to connect socket channel!");
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
                    throw new IOException("Failed to connect socket channel!");
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
    public int read(final ByteBuffer buffer) throws IOException {
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
            if (readableMessages.get() <= 0) {
                fillReceiveBuffer();
                UcxSelectableChannel.pollWorkerNonBlocking(worker);
            }
        }

        if (readableMessages.get() <= 0) {
            readLock.unlock();
            return 0;
        }

        final AtomicBoolean padding = new AtomicBoolean(true);
        final AtomicBoolean messageCompleted = new AtomicBoolean(false);
        final AtomicInteger readBytes = new AtomicInteger();
        int readFromBuffer = 0;

        do {
            readFromBuffer = receiveBuffer.read((msgTypeId, directBuffer, index, bufferLength) -> {
                // Get message length
                final int messageLength = directBuffer.getInt(index + HEADER_OFFSET_MESSAGE_LENGTH);
                // Get message offset
                final int offset = directBuffer.getInt(index + HEADER_OFFSET_READ_BYTES);
                LOGGER.debug("Message type id: [{}], Index: [{}], Buffer Length: [{}], Message Length: [{}], Offset: [{}]", msgTypeId, index, bufferLength, messageLength, offset);

                final int length = Math.min(buffer.remaining(), messageLength - offset);
                // Get message data
                directBuffer.getBytes(index + HEADER_OFFSET_MESSAGE_DATA + offset, buffer, length);
                // Put updated message offset
                directBuffer.putInt(index + HEADER_OFFSET_READ_BYTES, offset + length);

                if (offset + length == messageLength) {
                    messageCompleted.set(true);
                }

                padding.set(false);
                readBytes.set(length);
            }, 1);

            if (padding.get()) {
                LOGGER.debug("Read [{}] padding bytes from receive buffer", readFromBuffer);
                receiveBuffer.commitRead(readFromBuffer);
            }
        } while (padding.get());

        if (messageCompleted.get()) {
            final int readable = readableMessages.decrementAndGet();
            LOGGER.debug("Readable messages left: [{}]", readable);
            receiveBuffer.commitRead(readFromBuffer);
        }

        readLock.unlock();
        return readBytes.get();
    }

    @Override
    public long read(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
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
            readTotal += read(buffers[offset + i]);
            if(buffers[offset + i].remaining() > 0) {
                break;
            }
        }

        readLock.unlock();
        return readTotal;
    }

    @Override
    public int write(final ByteBuffer buffer) throws IOException {
        if (outputClosed || channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        writeLock.lock();
        int written = 0;

        do {
            final int length = Math.min(Math.min(buffer.remaining() + HEADER_LENGTH, sendBuffer.maxMessageLength()), bufferSliceLength);
            if (length <= HEADER_LENGTH) {
                LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", INSUFFICIENT_CAPACITY);
                if (isBlocking()) {
                    UcxSelectableChannel.pollWorkerNonBlocking(worker);
                    continue;
                } else {
                    writeLock.unlock();
                    return 0;
                }
            }

            final int index = sendBuffer.tryClaim(length);

            if (index < 0) {
                LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", index);
                if (isBlocking()) {
                    UcxSelectableChannel.pollWorkerNonBlocking(worker);
                    continue;
                } else {
                    writeLock.unlock();
                    return 0;
                }
            }

            // Put message length
            sendBuffer.buffer().putInt(index, length - HEADER_LENGTH);
            // Put number of read bytes (initially 0)
            sendBuffer.buffer().putInt(index + Integer.BYTES, 0);
            // Put message
            sendBuffer.buffer().putBytes(index + HEADER_LENGTH, buffer, buffer.position(), length - HEADER_LENGTH);
            // Advance position manually, since AtomicBuffer.putBytes() does not do it
            buffer.position(buffer.position() + length - HEADER_LENGTH);

            sendBuffer.commitWrite(index);
            endpoint.sendTaggedNonBlocking(sendBuffer.memoryAddress() + index, length, MESSAGE_TAG, sendCallback);

            if (isBlocking()) {
                try {
                    UcxSelectableChannel.pollWorkerNonBlocking(worker);
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
    public long write(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
        if (outputClosed || channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        writeLock.lock();
        int writtenTotal = 0;

        for (int i = 0; i < length; i++) {
            writtenTotal += write(buffers[offset + i]);
            if(buffers[offset + i].remaining() > 0) {
                break;
            }
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
    protected void implConfigureBlocking(final boolean blocking) throws IOException {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public int readyOps() {
        int readyOps = 0;

        if (connectable) {
            readyOps |= SelectionKey.OP_CONNECT;
        }

        if (connected && !outputClosed && sendBuffer.maxMessageLength() > HEADER_LENGTH) {
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
        int index = receiveBuffer.tryClaim(bufferSliceLength);

        while (index >= 0) {
            LOGGER.debug("Claimed part of the receive buffer (Index: [{}], Length: [{}])", index, bufferSliceLength);

            receiveBuffer.commitWrite(index);
            worker.recvTaggedNonBlocking(receiveBuffer.memoryAddress() + index, bufferSliceLength, MESSAGE_TAG, TAG_MASK, receiveCallback);

            index = receiveBuffer.tryClaim(bufferSliceLength);
        }
    }

    public void onConnection(boolean success) {
        if (success) {
            sendCallback = new SendCallback(this, sendBuffer);
            receiveCallback = new ReceiveCallback(this, readableMessages);
            fillReceiveBuffer();
            connected = true;
        } else {
            connected = false;
            connectionFailed = true;
        }

        connectable = true;
    }
}
