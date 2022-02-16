package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxEndpoint;
import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import de.hhu.bsinfo.hadronio.util.MemoryUtil;
import de.hhu.bsinfo.hadronio.util.MemoryUtil.Alignment;
import de.hhu.bsinfo.hadronio.util.MessageUtil;
import de.hhu.bsinfo.hadronio.util.RingBuffer;
import de.hhu.bsinfo.hadronio.util.TagUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.agrona.concurrent.ringbuffer.RingBuffer.INSUFFICIENT_CAPACITY;

public class HadronioSocketChannel extends SocketChannel implements HadronioSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioSocketChannel.class);
    private static final ByteBuffer[] SINGLE_BUFFER_ARRAY = new ByteBuffer[1];

    static final long CONNECTION_MAGIC_NUMBER = 0xC0FFEE00ADD1C7EDL;
    static final long FLUSH_ANSWER = 0xDEADBEEFDEADBEEFL;

    private final UcxEndpoint endpoint;
    private final Configuration configuration;

    private final RingBuffer sendBuffer;
    private final RingBuffer receiveBuffer;

    private final AtomicBuffer flushBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
    private final AtomicBoolean isFlushing = new AtomicBoolean();
    private final AtomicInteger readableMessages = new AtomicInteger();
    private int sendCounter;
    private long localTag;
    private long remoteTag;

    private boolean connectionPending = false;
    private boolean connectionFailed = false;
    private boolean connected = false;
    private boolean connectable = false;
    private boolean inputClosed = false;
    private boolean outputClosed = false;
    private boolean channelClosed = false;
    private int readyOps;

    public HadronioSocketChannel(final SelectorProvider provider, final UcxEndpoint endpoint) {
        super(provider);

        this.endpoint = endpoint;
        configuration = Configuration.getInstance();
        sendBuffer = new RingBuffer(configuration.getSendBufferLength());
        receiveBuffer = new RingBuffer(configuration.getReceiveBufferLength());
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
        if (!isConnected()) {
            throw new NotYetConnectedException();
        }

        if (channelClosed) {
            throw new ClosedChannelException();
        }
        
        LOGGER.info("Closing connection for input");

        inputClosed = true;
        return this;
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        if (!isConnected()) {
            throw new NotYetConnectedException();
        }

        if (channelClosed) {
            throw new ClosedChannelException();
        }

        LOGGER.info("Closing connection for output");

        outputClosed = true;
        return this;
    }

    @Override
    public Socket socket() {
        return new WrappingSocket(this);
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
    public synchronized boolean connect(final SocketAddress remoteAddress) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (isConnected()) {
            throw new AlreadyConnectedException();
        }

        if (connectionPending) {
            throw new ConnectionPendingException();
        }

        if (!(remoteAddress instanceof InetSocketAddress)) {
            throw new UnsupportedAddressTypeException();
        }

        connectionPending = true;
        LOGGER.info("Connecting to [{}]", remoteAddress);
        endpoint.connect((InetSocketAddress) remoteAddress);
        establishConnection();

        if (isBlocking()) {
            finishConnect();
        }

        return connected;
    }

    @Override
    public synchronized boolean finishConnect() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connectionPending) {
            throw new NoConnectionPendingException();
        }

        if (isBlocking()) {
            while (!connected && !connectionFailed) {
                endpoint.getWorker().progress();
                if (endpoint.getErrorState()) {
                    onConnection(false, 0, 0);
                }
            }
        }

        if (connectionFailed) {
            throw new IOException("Failed to connect socket channel!");
        }

        if (connectable) {
            connected = true;
            connectable = false;
        }

        return connected;
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!connected) {
            throw new NotYetConnectedException();
        }

        return endpoint.getRemoteAddress();
    }

    @Override
    public int read(final ByteBuffer buffer) throws IOException {
        if (isNotReadable()) {
            return -1;
        }

        synchronized (receiveBuffer) {
            if (isBlocking()) {
                return readBlocking(buffer);
            } else {
                return readNonBlocking(buffer);
            }
        }
    }

    @Override
    public long read(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
        if (isNotReadable()) {
            return -1;
        }

        synchronized (receiveBuffer) {
            int readTotal = 0;

            for (int i = 0; i < length; i++) {
                readTotal += read(buffers[offset + i]);
                if (buffers[offset + i].remaining() > 0) {
                    break;
                }
            }

            return readTotal;
        }
    }

    @Override
    public int write(final ByteBuffer buffer) throws IOException {
        if (isNotWriteable()) {
            return 0;
        }

        synchronized (sendBuffer) {
            SINGLE_BUFFER_ARRAY[0] = buffer;
            return (int) write(SINGLE_BUFFER_ARRAY, 0, 1);
        }
    }

    @Override
    public long write(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
        if (isNotWriteable()) {
            return 0;
        }

        synchronized (sendBuffer) {
            if (isBlocking()) {
                // Calculate full message length
                int totalLength = 0;
                for (int i = 0; i < length; i++) {
                    totalLength += buffers[offset + i].remaining();
                }

                // Call write repeatedly, until all bytes are written
                long totalWritten = 0;
                while (totalWritten < totalLength) {
                    long written = write(buffers, offset, length, true);
                    if (written == 0) {
                        endpoint.getWorker().progress();
                        if (endpoint.getErrorState()) {
                            throw new IOException("UCX endpoint has moved to error state!");
                        }
                    }

                    totalWritten += written;
                }

                return totalWritten;
            } else {
                return write(buffers, offset, length, false);
            }
        }
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        throw new IOException("Trying to get local address, which is not supported");
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
        LOGGER.info("Closing socket channel");
        channelClosed = true;
        inputClosed = true;
        outputClosed = true;
        connected = false;

        endpoint.close();
    }

    @Override
    protected void implConfigureBlocking(final boolean blocking) {
        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public void select() {
        // Handle error cases
        if (endpoint.getErrorState()) {
            if (isConnected()) {
                // An error has occurred and the connection is no longer usable. To notify the application about this,
                // the channel becomes readable, but every call to read() will immediately return -1.
                this.readyOps = SelectionKey.OP_READ;
            } else {
                // An error has occurred while connecting to a remote channel. The channel becomes connectable,
                // but finishConnect() will throw an IOException to notify the application about the failed connection attempt.
                onConnection(false, 0 ,0);
                this.readyOps = SelectionKey.OP_CONNECT;
            }

            return;
        }


        // If the connection is still valid, make sure the receiveBuffer is filled with requests
        if (isConnected()) {
            fillReceiveBuffer();
        }

        // Calculate ready operation set
        int readyOps = 0;

        if (connectable) {
            // Connection needs to be finished via finishConnect()
            readyOps |= SelectionKey.OP_CONNECT;
        }
        if (isConnected() && !outputClosed && !isFlushing.get() && sendBuffer.maxMessageLength() > MessageUtil.HEADER_LENGTH) {
            // Channel is writable, since there is place in the sendBuffer
            readyOps |= SelectionKey.OP_WRITE;
        }
        if (isConnected() && !inputClosed && readableMessages.get() > 0) {
            // Channel is readable, since there are unread messages in the receiveBuffer
            readyOps |= SelectionKey.OP_READ;
        }

        this.readyOps = readyOps;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    @Override
    public UcxWorker getWorker() {
        return endpoint.getWorker();
    }

    private void flush() {
        final long tag = TagUtil.setMessageType(localTag, TagUtil.MessageType.FLUSH);
        isFlushing.set(true);
        flushBuffer.putLong(0, 0);
        endpoint.receiveTaggedMessage(flushBuffer.addressOffset(), flushBuffer.capacity(), tag, TagUtil.TAG_MASK_FULL,true, false);
    }

    public void onConnection(final boolean success, long localTag, long remoteTag) {
        if (success) {
            this.localTag = localTag;
            this.remoteTag = remoteTag;

            endpoint.setSendCallback(() -> {
                LOGGER.debug("hadroNIO SendCallback called");
                final AtomicBoolean padding = new AtomicBoolean(true);
                int readFromBuffer;

                do {
                    readFromBuffer = sendBuffer.read((msgTypeId, buffer, index, length) -> {
                        LOGGER.debug("Message type id: [{}], Index: [{}], Length: [{}]", msgTypeId, index, length);
                        padding.set(false);
                    }, 1);

                    if (padding.get()) {
                        LOGGER.debug("Read [{}] padding bytes from send buffer", readFromBuffer);
                        sendBuffer.commitRead(readFromBuffer);
                    }
                } while (padding.get());

                sendBuffer.commitRead(readFromBuffer);
            });

            endpoint.setReceiveCallback(new ReceiveCallback(this, readableMessages, isFlushing, configuration.getFlushIntervalSize()));

            LOGGER.info("SocketChannel connected successfully (localTag: [0x{}], remoteTag: [0x{}])", Long.toHexString(localTag), Long.toHexString(remoteTag));

            if (isBlocking()) {
                connected = true;
            }

            fillReceiveBuffer();
        } else {
            connectionFailed = true;
        }

        if (!isBlocking()) {
            connectable = true;
        }
    }

    private void fillReceiveBuffer() {
        final long tag = TagUtil.setMessageType(localTag, TagUtil.MessageType.DEFAULT);
        int index = receiveBuffer.tryClaim(configuration.getBufferSliceLength());

        while (index >= 0) {
            LOGGER.debug("Claimed part of the receive buffer (Index: [{}], Length: [{}])", index, configuration.getBufferSliceLength());

            receiveBuffer.commitWrite(index);
            final boolean completed = endpoint.receiveTaggedMessage(receiveBuffer.memoryAddress() + index, configuration.getBufferSliceLength(), tag, TagUtil.TAG_MASK_FULL, true, false);
            LOGGER.debug("Receive request completed instantly: [{}]", completed);

            index = receiveBuffer.tryClaim(configuration.getBufferSliceLength());
        }
    }

    long getRemoteTag() {
        return remoteTag;
    }

    boolean isInputClosed() {
        return inputClosed;
    }

    boolean isOutputClosed() {
        return outputClosed;
    }

    UcxEndpoint getSocketChannelImplementation() {
        return endpoint;
    }

    void establishConnection() {
        final AtomicBuffer sendBuffer = MemoryUtil.allocateAligned(2 * Long.BYTES, Alignment.PAGE);
        final AtomicBuffer receiveBuffer = MemoryUtil.allocateAligned(2 * Long.BYTES, Alignment.PAGE);

        final long localId = TagUtil.generateId();
        sendBuffer.putLong(0, CONNECTION_MAGIC_NUMBER);
        sendBuffer.putLong(Long.BYTES, localId);

        final ConnectionCallback connectionCallback = new ConnectionCallback(this, receiveBuffer, localId);

        LOGGER.info("Exchanging tags to establish connection");
        endpoint.sendStream(sendBuffer.addressOffset(), sendBuffer.capacity(), connectionCallback);
        endpoint.receiveStream(receiveBuffer.addressOffset(), receiveBuffer.capacity(), connectionCallback);
    }

    private int readBlocking(final ByteBuffer target) throws IOException {
        while (readableMessages.get() <= 0) {
            fillReceiveBuffer();
            endpoint.getWorker().progress();

            if (endpoint.getErrorState()) {
                throw new IOException("UCX endpoint has moved to error state!");
            }
        }

        return readFromReceiveBuffer(target);
    }

    private int readNonBlocking(final ByteBuffer target) {
        if (readableMessages.get() <= 0) {
            return 0;
        }

        return readFromReceiveBuffer(target);
    }

    private int readFromReceiveBuffer(final ByteBuffer target) {
        final AtomicBoolean padding = new AtomicBoolean(true);
        final AtomicBoolean messageCompleted = new AtomicBoolean(false);
        final AtomicInteger readBytes = new AtomicInteger();
        int readFromBuffer;

        LOGGER.debug("Trying to read [{}] bytes (Readable messages: [{}])", target.remaining(), readableMessages.get());

        do {
            readFromBuffer = receiveBuffer.read((msgTypeId, sourceBuffer, sourceIndex, sourceBufferLength) -> {
                final int read = MessageUtil.readMessage(sourceBuffer, sourceIndex, target);
                final int remaining = MessageUtil.getRemainingBytes(sourceBuffer, sourceIndex);
                LOGGER.debug("Message type id: [{}], Index: [{}], Buffer Length: [{}], Read: [{}], Remaining: [{}]", msgTypeId, sourceIndex, sourceBufferLength, read, remaining);

                padding.set(false);
                readBytes.set(read);
                messageCompleted.set(remaining == 0);
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

        return readBytes.get();
    }

    private int writeBlocking(final ByteBuffer source) throws IOException {
        int written = 0;

        do {
            while (isFlushing.get()) {
                endpoint.getWorker().progress();
            }

            final int length = Math.min(Math.min(source.remaining() + MessageUtil.HEADER_LENGTH, sendBuffer.maxMessageLength()), configuration.getBufferSliceLength());
            if (length <= MessageUtil.HEADER_LENGTH) {
                LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", INSUFFICIENT_CAPACITY);
                endpoint.getWorker().progress();

                if (endpoint.getErrorState()) {
                    throw new IOException("UCX endpoint has moved to error state!");
                }

                continue;
            }

            final int index = sendBuffer.tryClaim(length);

            if (index < 0) {
                LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", index);
                endpoint.getWorker().progress();

                if (endpoint.getErrorState()) {
                    throw new IOException("UCX endpoint has moved to error state!");
                }

                continue;
            }

            MessageUtil.writeMessage(sendBuffer.buffer(), index, source, length - MessageUtil.HEADER_LENGTH);
            source.position(source.position() + length - MessageUtil.HEADER_LENGTH);
            sendBuffer.commitWrite(index);

            final long tag = TagUtil.setMessageType(remoteTag, TagUtil.MessageType.DEFAULT);
            final boolean blocking = isBlocking() && !source.hasRemaining();
            endpoint.sendTaggedMessage(sendBuffer.memoryAddress() + index, length, tag, true, blocking);

            if (endpoint.getErrorState()) {
                throw new IOException("UCX endpoint has moved to error state!");
            }

            if (++sendCounter % configuration.getFlushIntervalSize() == 0) {
                flush();
            }

            written += length - MessageUtil.HEADER_LENGTH;
        } while (source.hasRemaining());

        return written;
    }

    private int write(final ByteBuffer[] sources, final int offset, final int length, final boolean blocking) {
        // Do not send anything, while flushing; Too many dangling messages cause high memory usage by UCX
        if (isFlushing.get()) {
            return 0;
        }

        // Calculate full message length
        int sourcesLength = 0;
        for (int i = 0; i < length; i++) {
            sourcesLength += sources[offset + i].remaining();
        }

        if (sourcesLength == 0) {
            return 0;
        }

        // Claim space in send buffer
        // If the message is larger than a single buffer slice, we only claim a buffer slice and do not send the full message
        final int messageLength = Math.min(Math.min(sourcesLength + MessageUtil.HEADER_LENGTH, sendBuffer.maxMessageLength()), configuration.getBufferSliceLength());
        if (messageLength <= MessageUtil.HEADER_LENGTH) {
            LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", INSUFFICIENT_CAPACITY);
            return 0;
        }

        final int index = sendBuffer.tryClaim(messageLength);

        if (index < 0) {
            LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", index);
            return 0;
        }

        // Write message header
        MessageUtil.setMessageLength(sendBuffer.buffer(), index, messageLength - MessageUtil.HEADER_LENGTH);
        MessageUtil.setReadBytes(sendBuffer.buffer(), index, 0);

        // Copy message data from source buffers into send buffer
        int remaining = messageLength - MessageUtil.HEADER_LENGTH;
        int targetIndex = index + MessageUtil.MESSAGE_OFFSET_DATA;
        int lastBufferIndex = 0;
        int lastBufferPosition = 0;

        for (int i = 0; remaining > 0; i++) {
            final ByteBuffer sourceBuffer = sources[offset + i];
            final int currentLength = Math.min(sourceBuffer.remaining(), remaining);
            if (sourceBuffer.remaining() == 0) {
                continue;
            }

            LOGGER.debug("Copying source buffer into send buffer (Buffer: [{}/{}], Position: [{}/{}], Length: [{}], Remaining: [{}])",
                offset + i + 1, length, sourceBuffer.position(), sourceBuffer.limit(), currentLength, remaining);
            sendBuffer.buffer().putBytes(targetIndex, sourceBuffer, sourceBuffer.position(), currentLength);

            lastBufferIndex = i;
            lastBufferPosition = sourceBuffer.position() + currentLength;
            targetIndex += currentLength;
            remaining -= currentLength;
        }

        sendBuffer.commitWrite(index);

        // Update source buffer positions afterwards
        // We cannot do it inside the copy loop, because it is possible, that the array contains the same buffer multiple times
        for (int i = 0; i < lastBufferIndex; i++) {
            final ByteBuffer buffer = sources[offset + i];
            buffer.position(buffer.limit());
        }
        sources[lastBufferIndex].position(lastBufferPosition);

        // Send message via endpoint
        final long tag = TagUtil.setMessageType(remoteTag, TagUtil.MessageType.DEFAULT);
        final boolean completed = endpoint.sendTaggedMessage(sendBuffer.memoryAddress() + index, messageLength, tag, true, blocking);
        LOGGER.debug("Send request completed instantly: [{}]", completed);

        // Flush, if necessary
        if (++sendCounter % configuration.getFlushIntervalSize() == 0) {
            flush();
        }

        return messageLength - MessageUtil.HEADER_LENGTH;
    }

    private boolean isNotReadable() throws ClosedChannelException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!isConnected()) {
            throw new NotYetConnectedException();
        }

        if (endpoint.getErrorState() && readableMessages.get() == 0) {
            return true;
        }

        return inputClosed;
    }

    private boolean isNotWriteable() throws ClosedChannelException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!isConnected()) {
            throw new NotYetConnectedException();
        }

        return outputClosed;
    }
}
