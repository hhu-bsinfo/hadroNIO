package de.hhu.bsinfo.hadronio.util;

import de.hhu.bsinfo.hadronio.generated.DebugConfig;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class MessageUtil {

    public static final int HEADER_LENGTH = 3 + 3 + (DebugConfig.DEBUG ? Short.BYTES : 0);
    public static final int MESSAGE_OFFSET_LENGTH = 0;
    public static final int MESSAGE_OFFSET_READ_BYTES = 3;
    public static final int MESSAGE_OFFSET_SEQUENCE_NUMBER = 2 * 3;
    public static final int MESSAGE_OFFSET_DATA = HEADER_LENGTH;

    public static int getMessageLength(final MutableDirectBuffer buffer, final int index) {
        return read24BitInt(buffer, index + MESSAGE_OFFSET_LENGTH);
    }

    public static int getReadBytes(final MutableDirectBuffer buffer, final int index) {
        return read24BitInt(buffer, index + MESSAGE_OFFSET_READ_BYTES);
    }

    public static short getSequenceNumber(final MutableDirectBuffer buffer, final int index) {
        return buffer.getShort(index + MESSAGE_OFFSET_SEQUENCE_NUMBER);
    }

    public static void getMessageData(final MutableDirectBuffer sourceBuffer, final int sourceIndex, final ByteBuffer targetBuffer, final int length, final int offset) {
        sourceBuffer.getBytes(sourceIndex + offset + MESSAGE_OFFSET_DATA, targetBuffer, length);
    }

    public static int getRemainingBytes(final MutableDirectBuffer buffer, final int index) {
        return getMessageLength(buffer, index) - getReadBytes(buffer, index);
    }

    public static int readMessage(final MutableDirectBuffer sourceBuffer, final int sourceIndex, final ByteBuffer targetBuffer) {
        final var messageLength = getMessageLength(sourceBuffer, sourceIndex);
        final var offset = getReadBytes(sourceBuffer, sourceIndex);
        final var length = Math.min(targetBuffer.remaining(), messageLength - offset);

        getMessageData(sourceBuffer, sourceIndex, targetBuffer, length, offset);
        setReadBytes(sourceBuffer, sourceIndex, offset + length);

        return length;
    }

    public static void setMessageLength(final MutableDirectBuffer buffer, final int index, final int value) {
        write24BitInt(buffer, index + MESSAGE_OFFSET_LENGTH, value);
    }

    public static void setReadBytes(final MutableDirectBuffer buffer, final int index, final int value) {
        write24BitInt(buffer, index + MESSAGE_OFFSET_READ_BYTES, value);
    }

    public static void setSequenceNumber(final MutableDirectBuffer buffer, final int index, final short value) {
        buffer.putShort(index + MESSAGE_OFFSET_SEQUENCE_NUMBER, value);
    }

    public static void setMessageData(final MutableDirectBuffer targetBuffer, final int targetIndex, final ByteBuffer sourceBuffer, final int messageLength) {
        targetBuffer.putBytes(targetIndex + MESSAGE_OFFSET_DATA, sourceBuffer, sourceBuffer.position(), messageLength);
    }

    private static int read24BitInt(final MutableDirectBuffer buffer, final int index) {
        return (buffer.getShort(index) & 0xffff) | ((buffer.getByte(index + 2) & 0xff) << 16);
    }

    private static void write24BitInt(final MutableDirectBuffer buffer, final int index, final int value) {
        buffer.putShort(index, (short) value);
        buffer.putByte(index + 2, (byte) (value >> 16));
    }
}
