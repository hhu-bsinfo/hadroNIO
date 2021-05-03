package de.hhu.bsinfo.hadronio;

import org.agrona.BitUtil;

class Configuration {

    private static final int MIN_SEND_BUFFER_LENGTH = 128;
    private static final int MIN_RECEIVE_BUFFER_LENGTH = 128;
    private static final int MIN_BUFFER_SLICE_LENGTH = 32;
    private static final int MIN_FLUSH_INTERVAL_SIZE = 128;

    private static final int DEFAULT_SEND_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_RECEIVE_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_BUFFER_SLICE_LENGTH = 32 * 1024;
    private static final int DEFAULT_FLUSH_INTERVAL_SIZE = 1024;
    private static final String DEFAULT_PROVIDER_CLASS = "de.hhu.bsinfo.hadronio.jucx.JucxProvider";

    private final int sendBufferLength;
    private final int receiveBufferLength;
    private final int bufferSliceLength;
    private final int flushIntervalSize;
    private final String providerClass;

    public static Configuration getInstance() throws IllegalArgumentException {
        final int sendBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.SEND_BUFFER_LENGTH", String.valueOf(DEFAULT_SEND_BUFFER_LENGTH)));
        final int receiveBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.RECEIVE_BUFFER_LENGTH", String.valueOf(DEFAULT_RECEIVE_BUFFER_LENGTH)));
        final int bufferSliceLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.BUFFER_SLICE_LENGTH", String.valueOf(DEFAULT_BUFFER_SLICE_LENGTH)));
        final int flushIntervalSize = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.FLUSH_INTERVAL_SIZE", String.valueOf(DEFAULT_FLUSH_INTERVAL_SIZE)));
        final String providerClass = System.getProperty("de.hhu.bsinfo.hadronio.Configuration.PROVIDER_CLASS", DEFAULT_PROVIDER_CLASS);

        checkConfiguration(sendBufferLength, receiveBufferLength, bufferSliceLength, flushIntervalSize, providerClass);
        return new Configuration(sendBufferLength, receiveBufferLength, bufferSliceLength + HadronioSocketChannel.HEADER_LENGTH, flushIntervalSize, providerClass);
    }

    private static void checkConfiguration(final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength, final int flushIntervalSize, final String providerClass) throws IllegalArgumentException {
        if (sendBufferLength < MIN_SEND_BUFFER_LENGTH) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least " + MIN_SEND_BUFFER_LENGTH + " byte!");
        }

        if (receiveBufferLength < MIN_RECEIVE_BUFFER_LENGTH) {
            throw new IllegalArgumentException("RECEIVE_BUFFER_LENGTH must be a at least " + MIN_RECEIVE_BUFFER_LENGTH + " byte!");
        }

        if (bufferSliceLength < MIN_BUFFER_SLICE_LENGTH) {
            throw new IllegalArgumentException("BUFFER_SLICE_LENGTH must be a at least " + MIN_BUFFER_SLICE_LENGTH + " byte!");
        }

        if (flushIntervalSize < MIN_FLUSH_INTERVAL_SIZE) {
            throw new IllegalArgumentException("FLUSH_INTERVAL_SIZE must be a at least " + MIN_FLUSH_INTERVAL_SIZE + "!");
        }

        if (!BitUtil.isPowerOfTwo(bufferSliceLength)) {
            throw new IllegalArgumentException("BUFFER_SLICE_LENGTH must be a power of 2");
        }

        if (!BitUtil.isPowerOfTwo(sendBufferLength)) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a power of 2");
        }

        if (!BitUtil.isPowerOfTwo(receiveBufferLength)) {
            throw new IllegalArgumentException("RECEIVE_BUFFER_LENGTH must be a power of 2");
        }

        if (sendBufferLength < 2 * bufferSliceLength) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least twice as high as BUFFER_SLICE_LENGTH!");
        }

        if (receiveBufferLength < 2 * bufferSliceLength) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least twice as high as RECEIVE_BUFFER_LENGTH!");
        }
    }

    private Configuration(final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength, final int flushIntervalSize, final String providerClass) {
        this.sendBufferLength = sendBufferLength;
        this.receiveBufferLength = receiveBufferLength;
        this.bufferSliceLength = bufferSliceLength;
        this.flushIntervalSize = flushIntervalSize;
        this.providerClass = providerClass;
    }

    int getSendBufferLength() {
        return sendBufferLength;
    }

    int getReceiveBufferLength() {
        return receiveBufferLength;
    }

    int getBufferSliceLength() {
        return bufferSliceLength;
    }

    int getFlushIntervalSize() {
        return flushIntervalSize;
    }

    String getProviderClass() {
        return providerClass;
    }

    @Override
    public String toString() {
        return "Configuration(" +
                "sendBufferSize=" + sendBufferLength +
                ",receiveBufferSize=" + receiveBufferLength +
                ",bufferSliceLength=" + bufferSliceLength +
                ",flushIntervalSize=" + flushIntervalSize +
                ",providerClass=" + providerClass +
                ")";
    }
}
