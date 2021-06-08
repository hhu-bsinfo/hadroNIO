package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.RingBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
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

import static org.agrona.concurrent.ringbuffer.RingBuffer.INSUFFICIENT_CAPACITY;

public class HadronioSocketChannel extends SocketChannel implements HadronioSelectableChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioSocketChannel.class);

    static final long TAG_TYPE_MESSAGE = 0x0000000100000000L;
    static final long TAG_TYPE_FLUSH = 0x0000000200000000L;
    static final long TAG_MASK_TARGET = 0x00000000ffffffffL;
    static final long TAG_MASK_TYPE = 0xffffffff00000000L;

    static final long FLUSH_ANSWER = 0xDEADBEEFDEAFBEEFL;

    static final int HEADER_LENGTH = Integer.BYTES * 2;
    static final int HEADER_OFFSET_MESSAGE_LENGTH = 0;
    static final int HEADER_OFFSET_READ_BYTES = Integer.BYTES;
    static final int HEADER_OFFSET_MESSAGE_DATA = Integer.BYTES * 2;

    private final UcxSocketChannel socketChannel;
    private final UcxWorker worker;
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
    private boolean connectable = false;
    private boolean inputClosed = false;
    private boolean outputClosed = false;
    private boolean channelClosed = false;
    private int readyOps;

    public HadronioSocketChannel(final SelectorProvider provider, final UcxSocketChannel socketChannel, final UcxWorker worker) {
        super(provider);

        this.socketChannel = socketChannel;
        this.worker = worker;
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
        if (!socketChannel.isConnected()) {
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
        if (!socketChannel.isConnected()) {
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
        return socketChannel.isConnected();
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

        if (socketChannel.isConnected()) {
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
        socketChannel.connect((InetSocketAddress) remoteAddress, new ConnectionCallback(this));

        if (isBlocking()) {
            finishConnect();
        }

        return socketChannel.isConnected();
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
            while (!socketChannel.isConnected()) {
                worker.poll(true);
            }
        }

        if (socketChannel.isConnected()) {
            connectable = false;
        } else if (connectionFailed) {
            throw new IOException("Failed to connect socket channel!");
        }

        return socketChannel.isConnected();
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

        if (!socketChannel.isConnected()) {
            throw new NotYetConnectedException();
        }

        if (inputClosed) {
            return -1;
        }

        synchronized (receiveBuffer) {
            if (isBlocking()) {
                while (readableMessages.get() <= 0) {
                    fillReceiveBuffer();
                    worker.poll(false);
                }
            }

            if (readableMessages.get() <= 0) {
                return 0;
            }

            final AtomicBoolean padding = new AtomicBoolean(true);
            final AtomicBoolean messageCompleted = new AtomicBoolean(false);
            final AtomicInteger readBytes = new AtomicInteger();
            int readFromBuffer;

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

            return readBytes.get();
        }
    }

    @Override
    public long read(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
        if (channelClosed) {
            throw new ClosedChannelException();
        }

        if (!socketChannel.isConnected()) {
            throw new NotYetConnectedException();
        }

        if (inputClosed) {
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
        if (outputClosed || channelClosed) {
            throw new ClosedChannelException();
        }

        if (!socketChannel.isConnected()) {
            throw new NotYetConnectedException();
        }

        synchronized (sendBuffer) {
            int written = 0;

            do {
                final int length = Math.min(Math.min(buffer.remaining() + HEADER_LENGTH, sendBuffer.maxMessageLength()), configuration.getBufferSliceLength());
                if (length <= HEADER_LENGTH) {
                    LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", INSUFFICIENT_CAPACITY);
                    if (isBlocking()) {
                        worker.poll(true);
                        continue;
                    }
                    return written;
                }

                final int index = sendBuffer.tryClaim(length);

                if (index < 0) {
                    LOGGER.debug("Unable to claim space in the send buffer (Error: [{}])", index);
                    if (isBlocking()) {
                        worker.poll(true);
                        continue;
                    }
                    return written;
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

                final long tag = remoteTag | TAG_TYPE_MESSAGE;
                final boolean blocking = !configuration.useWorkerPollThread() && isBlocking() && !buffer.hasRemaining();
                final boolean completed = socketChannel.sendTaggedMessage(sendBuffer.memoryAddress() + index, length, tag, true, blocking);
                LOGGER.debug("Send request completed instantly: [{}]", completed);

                if (++sendCounter % configuration.getFlushIntervalSize() == 0) {
                    flush();
                }

                written += length - HEADER_LENGTH;
            } while (isBlocking() && buffer.hasRemaining());

            return written;
        }
    }

    @Override
    public long write(final ByteBuffer[] buffers, final int offset, final int length) throws IOException {
        if (outputClosed || channelClosed) {
            throw new ClosedChannelException();
        }

        if (!socketChannel.isConnected()) {
            throw new NotYetConnectedException();
        }

        synchronized (sendBuffer) {
            int writtenTotal = 0;

            for (int i = 0; i < length; i++) {
                writtenTotal += write(buffers[offset + i]);
                if (buffers[offset + i].remaining() > 0) {
                    break;
                }
            }

            return writtenTotal;
        }
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
        socketChannel.close();
        sendBuffer.close();
        receiveBuffer.close();
    }

    @Override
    protected void implConfigureBlocking(final boolean blocking) throws IOException {
        if (!blocking && Configuration.getInstance().useWorkerPollThread()) {
            throw new IllegalArgumentException("Non-blocking socket channels are not supported when using the worker poll thread!");
        }

        LOGGER.info("Socket channel is now configured to be [{}]", blocking ? "BLOCKING" : "NON-BLOCKING");
    }

    @Override
    public void select() throws IOException {
        if (socketChannel.isConnected()) {
            fillReceiveBuffer();
        }

        int readyOps = 0;
        if (connectable) {
            readyOps |= SelectionKey.OP_CONNECT;
        }
        if (socketChannel.isConnected() && !outputClosed && !isFlushing.get() && sendBuffer.maxMessageLength() > HEADER_LENGTH) {
            readyOps |= SelectionKey.OP_WRITE;
        }
        if (!inputClosed && readableMessages.get() > 0) {
            readyOps |= SelectionKey.OP_READ;
        }

        this.readyOps = readyOps;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    private void flush() throws IOException {
        final long tag = localTag | TAG_TYPE_FLUSH;
        isFlushing.set(true);
        flushBuffer.putLong(0, 0);
        socketChannel.receiveTaggedMessage(flushBuffer.addressOffset(), flushBuffer.capacity(), tag, TAG_MASK_TARGET | TAG_MASK_TYPE,true, false);
    }

    public void onConnection(final boolean success, long localTag, long remoteTag) {
        if (success) {
            this.localTag = localTag;
            this.remoteTag = remoteTag;
            socketChannel.setSendCallback(new SendCallback(this, sendBuffer));
            socketChannel.setReceiveCallback(new ReceiveCallback(this, readableMessages, isFlushing, configuration.getFlushIntervalSize()));

            LOGGER.info("SocketChannel connected successfully (localTag: [0x{}], remoteTag: [0x{}])", Long.toHexString(localTag), Long.toHexString(remoteTag));

            try {
                fillReceiveBuffer();
            } catch (IOException e) {
                LOGGER.error("Failed to fill receive buffer", e);
            }
        } else {
            connectionFailed = true;
        }

        if (!isBlocking()) {
            connectable = true;
        }
    }

    private void fillReceiveBuffer() throws IOException {
        final long tag = localTag | TAG_TYPE_MESSAGE;
        int index = receiveBuffer.tryClaim(configuration.getBufferSliceLength());

        while (index >= 0) {
            LOGGER.debug("Claimed part of the receive buffer (Index: [{}], Length: [{}])", index, configuration.getBufferSliceLength());

            receiveBuffer.commitWrite(index);
            final boolean completed = socketChannel.receiveTaggedMessage(receiveBuffer.memoryAddress() + index, configuration.getBufferSliceLength(), tag, TAG_MASK_TARGET | TAG_MASK_TYPE, true, false);
            LOGGER.debug("Receive request completed instantly: [{}]", completed);

            index = receiveBuffer.tryClaim(configuration.getBufferSliceLength());
        }
    }

    long getLocalTag() {
        return localTag;
    }

    long getRemoteTag() {
        return remoteTag;
    }

    UcxSocketChannel getSocketChannelImplementation() {
        return socketChannel;
    }
}
