package de.hhu.bsinfo.hadronio.util;

import org.agrona.BitUtil;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;

import java.io.Closeable;

import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.broadcast.RecordDescriptor.PADDING_MSG_TYPE_ID;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBuffer.INSUFFICIENT_CAPACITY;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.*;

/**
 * A ring buffer used for storing requests.
 * This implementation is a modified version of {@link org.agrona.concurrent.ringbuffer.OneToOneRingBuffer}.
 */
public class RingBuffer implements Closeable {

    private static final int REQUEST_MESSAGE_ID = 1;

    /**
     * This buffer's maximum capacity in bytes.
     */
    private final int capacity;

    /**
     * The index within our backing buffer at which the head position is stored.
     */
    private final int headPositionIndex;

    /**
     * The index within our backing buffer at which the cached head position is stored.
     */
    private final int headCachePositionIndex;

    /**
     * The index within our backing buffer at which the tail position is stored.
     */
    private final int tailPositionIndex;

    /**
     * The underlying buffer used for storing data.
     */
    private final AtomicBuffer buffer;

    /**
     * Bitmask used to keep indices within the buffer's bounds.
     */
    private final int indexMask;

    public RingBuffer(final int size) {
        // Allocate a new page-aligned buffer
        buffer = MemoryUtil.allocateAligned(size + TRAILER_LENGTH, MemoryUtil.Alignment.PAGE);

        // Store the buffer's actual capacity
        capacity = buffer.capacity() - TRAILER_LENGTH;
        indexMask = capacity - 1;

        // Verify the buffer is correctly aligned
        buffer.verifyAlignment();

        // Remember positions at which indices are stored
        headPositionIndex = capacity + HEAD_POSITION_OFFSET;
        headCachePositionIndex = capacity + HEAD_CACHE_POSITION_OFFSET;
        tailPositionIndex = capacity + TAIL_POSITION_OFFSET;
    }

    public int read(final MessageHandler handler, final int limit) {
        // Keep track of the messages we already read
        int messagesRead = 0;

        // Retrieve our current position within the buffer
        final AtomicBuffer buffer = this.buffer;
        final int headPositionIndex = this.headPositionIndex;
        final long head = buffer.getLong(headPositionIndex);
        final int capacity = this.capacity;
        final int headIndex = (int) head & indexMask;
        final int maxBlockLength = capacity - headIndex;

        // Keep track of the number of bytes we read
        int bytesRead = 0;
        while ((bytesRead < maxBlockLength) && (messagesRead < limit)) {
            final int recordIndex = headIndex + bytesRead;
            final int recordLength = buffer.getIntVolatile(lengthOffset(recordIndex));

            // If this record has not been committed yet, we have to abort
            if (recordLength <= 0) {
                break;
            }

            // Increment the number of bytes processed
            bytesRead += align(recordLength, ALIGNMENT);

            // Skip this record if it represents padding
            final int messageTypeId = buffer.getInt(typeOffset(recordIndex));
            if (messageTypeId == PADDING_MSG_TYPE_ID) {
                continue;
            }

            handler.onMessage(messageTypeId, buffer, recordIndex + HEADER_LENGTH, recordLength - HEADER_LENGTH);
            messagesRead++;
        }

        // Return the number of bytes read so the consumer can commit it later
        return bytesRead;
    }

    public void commitRead(final int bytes) {
        final AtomicBuffer buffer = this.buffer;
        final int headPositionIndex = this.headPositionIndex;
        final long head = buffer.getLong(headPositionIndex);

        buffer.putLongOrdered(headPositionIndex, head + bytes);
    }

    public int tryClaim(final int length) {
        final AtomicBuffer buffer = this.buffer;

        // Calculate the required size in bytes
        final int recordLength = length + HEADER_LENGTH;

        // Claim the required space
        final int recordIndex = claim(buffer, recordLength);

        // Check if space was claimed successfully
        if (recordIndex == INSUFFICIENT_CAPACITY) {
            return INSUFFICIENT_CAPACITY;
        }

        // Block claimed space
        buffer.putIntOrdered(lengthOffset(recordIndex), -recordLength);
        UnsafeAccess.UNSAFE.storeFence();
        buffer.putInt(typeOffset(recordIndex), REQUEST_MESSAGE_ID);

        // Return the index at which the producer may write its request
        return encodedMsgOffset(recordIndex);
    }

    public void commitWrite(final int index) {
        final AtomicBuffer buffer = this.buffer;

        // Calculate the request index and length
        final int recordIndex = computeRecordIndex(index);
        final int recordLength = verifyClaimedSpaceNotReleased(buffer, recordIndex);

        // Commit the request
        buffer.putIntOrdered(lengthOffset(recordIndex), -recordLength);
    }

    private int claim(final AtomicBuffer buffer, final int length) {

        // Calculate the required space to claim
        final int required = BitUtil.align(length, ALIGNMENT);

        // This buffer's capacity
        final int total = capacity;

        // The index at which the cached head position is stored
        final int headCachePosition = headCachePositionIndex;

        // Mask used to keep indices within bounds
        final int mask = indexMask;

        long head = buffer.getLongVolatile(headCachePosition);
        final long tail = buffer.getLongVolatile(tailPositionIndex);
        final int available = total - (int) (tail - head);

        if (required > available) { // If the required size is less than the cached available space left
            // Calculate available space using the head position
            head = buffer.getLongVolatile(headPositionIndex);

            if (required > (total - (int) (tail - head))) {
                // If the required size is less than the current available space left
                return INSUFFICIENT_CAPACITY;
            }

            // Update the cached head position
            buffer.putLongOrdered(headCachePosition, head);
        }

        // At this point we know that there is a chunk of memory at least the size we requested

        // Try to acquire the required space
        int padding = 0;
        int tailIndex = (int) tail & mask;
        final int remaining = total - tailIndex;

        if (required > remaining) { // If the space between the tail and the upper bound is not sufficient
            // Wrap around the head index
            int headIndex = (int) head & mask;

            if (required > headIndex) {  // If there is not enough space at the beginning of our buffer
                // Update our head index for one last try
                head = buffer.getLongVolatile(headPositionIndex);
                headIndex = (int) head & mask;
                if (required > headIndex) {
                    return INSUFFICIENT_CAPACITY;
                }

                // Update the cached head position
                buffer.putLongOrdered(headCachePosition, head);
            }

            padding = remaining;
        }

        buffer.putLongOrdered(tailPositionIndex, tail + required + padding);

        if (padding != 0) {
            buffer.putIntOrdered(lengthOffset(tailIndex), -padding);
            UnsafeAccess.UNSAFE.storeFence();

            buffer.putInt(typeOffset(tailIndex), PADDING_MSG_TYPE_ID);
            buffer.putIntOrdered(lengthOffset(tailIndex), padding);

            // If there was padding at the end of the buffer
            // our claimed space starts at index 0
            tailIndex = 0;
        }

        return tailIndex;
    }

    public int size() {
        final AtomicBuffer buffer = this.buffer;
        final int headPositionIndex = this.headPositionIndex;
        final int tailPositionIndex = this.tailPositionIndex;

        long headBefore;
        long tail;
        long headAfter = buffer.getLongVolatile(headPositionIndex);

        do {
            headBefore = headAfter;
            tail = buffer.getLongVolatile(tailPositionIndex);
            headAfter = buffer.getLongVolatile(headPositionIndex);
        } while (headAfter != headBefore);

        final long size = tail - headAfter;
        if (size < 0)  {
            return 0;
        }
        else if (size > capacity)  {
            return capacity;
        }

        return (int) size;
    }

    public int maxMessageLength() {
        final int remaining = capacity - size();
        final int alignmentAddend = remaining - BitUtil.align(remaining, ALIGNMENT);
        final int length = remaining - alignmentAddend - HEADER_LENGTH;

        return Math.max(length, 0);
    }

    public long memoryAddress() {
        return buffer.addressOffset();
    }

    public AtomicBuffer buffer() {
        return buffer;
    }

    private int computeRecordIndex(final int index) {
        final int recordIndex = index - HEADER_LENGTH;
        if (recordIndex < 0 || recordIndex > (capacity - HEADER_LENGTH))
        {
            throw new IllegalArgumentException("Invalid message index " + index + "!");
        }

        return recordIndex;
    }

    private int verifyClaimedSpaceNotReleased(final AtomicBuffer buffer, final int recordIndex) {
        final int recordLength = buffer.getInt(lengthOffset(recordIndex));
        if (recordLength < 0)
        {
            return recordLength;
        }

        throw new IllegalStateException("Claimed space previously " + (PADDING_MSG_TYPE_ID == buffer.getInt(typeOffset(recordIndex)) ? "aborted" : "committed" + "!"));
    }

    @Override
    public void close() {
        MemoryUtil.free(buffer);
    }
}
