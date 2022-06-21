package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import site.ycsb.ByteIterator;

public class YcsbObject {

    private final int fieldSize;
    private final Iterator[] fieldIterators;
    private byte[] data;

    public YcsbObject(final int fieldCount, final int fieldSize) {
        this.fieldSize = fieldSize;
        data = new byte[fieldCount * fieldSize];
        fieldIterators = new Iterator[fieldCount];

        for (int i = 0; i < fieldIterators.length; i++) {
            fieldIterators[i] = new Iterator(i * fieldSize);
        }
    }
    public Iterator getFieldIterator(final String fieldKey) {
        // Field keys always look like "fieldX", where X is an increasing number
        final int fieldId = Integer.parseInt(fieldKey.substring(5));
        return getFieldIterator(fieldId);
    }

    public Iterator getFieldIterator(final int fieldId) {
        fieldIterators[fieldId].reset();
        return fieldIterators[fieldId];
    }

    public void setFieldValue(final String fieldKey, final ByteIterator iterator) {
        // Field keys always look like "fieldX", where X is an increasing number
        final int fieldId = Integer.parseInt(fieldKey.substring(5));
        final int offset = fieldId * fieldSize;

        int i = 0;
        while (iterator.hasNext()) {
            data[offset + i++] = iterator.nextByte();
        }
    }

    public void setData(final byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public class Iterator extends ByteIterator {
        private final int startOffset;
        private int iteratorPos;

        public Iterator(final int startOffset) {
            this.startOffset = startOffset;
        }

        public void reset() {
            iteratorPos = startOffset;
        }

        @Override
        public boolean hasNext() {
            return iteratorPos - startOffset < fieldSize;
        }

        @Override
        public byte nextByte() {
            return data[startOffset + iteratorPos++];
        }

        @Override
        public long bytesLeft() {
            return fieldSize - (iteratorPos - startOffset);
        }
    }
}
