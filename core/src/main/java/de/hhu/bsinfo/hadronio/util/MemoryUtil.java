package de.hhu.bsinfo.hadronio.util;

import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.PrintStream;

public class MemoryUtil {

    private static final int LINE_LENGTH = 16;
    private static final String LINE_SEPARATOR_CHARACTER = "-";
    private static final String COLUMN_SEPARATOR_CHARACTER = "|";
    private static final String HEXDUMP_HEADER = "  OFFSET  | 0  1  2  3  4  5  6  7   8  9  A  B  C  D  E  F|   ANSI ASCII   ";
    private static final String LINE_SEPARATOR = LINE_SEPARATOR_CHARACTER.repeat(HEXDUMP_HEADER.length());

    public enum Alignment {
        TYPE    (0x0008),
        CACHE   (0x0040),
        PAGE    (0x1000);

        private final int alignment;

        Alignment(final int alignment) {
            this.alignment = alignment;
        }

        public int value() {
            return alignment;
        }
    }

    public static AtomicBuffer allocateAligned(final int size, final Alignment alignment) {
        return new UnsafeBuffer(BufferUtil.allocateDirectAligned(size, alignment.value()));
    }

    public static void dumpBuffer(final MutableDirectBuffer buffer, final int index, final int length, final PrintStream stream) {
        int offset = index;
        int bytes = length;

        stream.println();
        stream.println(HEXDUMP_HEADER);
        stream.println(LINE_SEPARATOR);

        while (bytes > 0) {
            final int lineLength = Math.min(bytes, LINE_LENGTH);

            // Print memory address
            stream.printf(" %08X |", buffer.addressOffset() + offset);

            // Print bytes
            for (int i = 0; i < LINE_LENGTH; i++) {
                if (i < lineLength) {
                    stream.printf("%02X", buffer.getByte(offset + i));
                } else {
                    stream.print("  ");
                }

                if (i == 7) {
                    stream.print("  ");
                } else if (i != LINE_LENGTH - 1) {
                    stream.print(" ");
                }
            }

            stream.print(COLUMN_SEPARATOR_CHARACTER);

            // Print characters
            for (int i = 0; i < LINE_LENGTH; i++) {
                if (i < lineLength) {
                    stream.printf("%c", sanitize(buffer.getByte(offset + i)));
                } else {
                    stream.print(" ");
                }
            }

            stream.println();

            offset += lineLength;
            bytes -= lineLength;
        }

        stream.println();
    }

    private static char sanitize(final byte value) {
        if (value < 0x30 || value > 0x7E) {
            return '.';
        }

        return (char) value;
    }
}

