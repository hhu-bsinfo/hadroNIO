package de.hhu.bsinfo.hadronio.util;

import org.agrona.BufferUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

class MemoryUtil {

    enum Alignment {
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

    public static AtomicBuffer allocateAligned(int size, Alignment alignment) {
        return new UnsafeBuffer(BufferUtil.allocateDirectAligned(size, alignment.value()));
    }

    public static AtomicBuffer wrap(long address, int size) {
        return new UnsafeBuffer(address, size);
    }
}

