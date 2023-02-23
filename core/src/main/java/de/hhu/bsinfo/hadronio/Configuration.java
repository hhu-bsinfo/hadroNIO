package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.util.MessageUtil;
import org.agrona.BitUtil;

import java.lang.management.ManagementFactory;

class Configuration {

    enum PollMethod {
        BUSY_POLLING,
        EPOLL,
        DYNAMIC
    }

    private static final Configuration instance = getInstance();

    private static final int MIN_SEND_BUFFER_LENGTH = 128;
    private static final int MIN_RECEIVE_BUFFER_LENGTH = 128;
    private static final int MIN_BUFFER_SLICE_LENGTH = 32;
    private static final int MIN_FLUSH_INTERVAL_SIZE = 128;

    private static final int DEFAULT_SEND_BUFFER_LENGTH = 8 * 1024 * 1024;
    private static final int DEFAULT_RECEIVE_BUFFER_LENGTH = 8 * 1024 * 1024;
    private static final int DEFAULT_BUFFER_SLICE_LENGTH = 64 * 1024;
    private static final int DEFAULT_FLUSH_INTERVAL_SIZE = 1024;

    private static final int DEFAULT_BUSY_POLL_TIMEOUT_NANOS = 20000;
    private static final String DEFAULT_POLL_METHOD = "BUSY_POLLING";

    private final int sendBufferLength;
    private final int receiveBufferLength;
    private final int bufferSliceLength;
    private final int flushIntervalSize;

    private final int busyPollTimeoutNanos;
    private final PollMethod pollMethod;

    private final String providerClass;

    private static String getDefaultProviderClass() {
        final boolean previewEnabled = ManagementFactory.getRuntimeMXBean().getInputArguments().contains("--enable-preview");
        final int javaVersion = Integer.parseInt(System.getProperty("java.specification.version"));
        boolean infinileapAvailable;
        try {
            Class.forName("de.hhu.bsinfo.hadronio.infinileap.InfinileapProvider",  false, ClassLoader.getSystemClassLoader());
            infinileapAvailable = true;
        } catch (ClassNotFoundException e) {
            infinileapAvailable = false;
        }

        if (javaVersion == 19 && previewEnabled && infinileapAvailable) {
            return "de.hhu.bsinfo.hadronio.infinileap.InfinileapProvider";
        }

        return "de.hhu.bsinfo.hadronio.jucx.JucxProvider";
    }

    static Configuration getInstance() throws IllegalArgumentException {
        if (instance != null) {
            return instance;
        }

        final int sendBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.SEND_BUFFER_LENGTH", String.valueOf(DEFAULT_SEND_BUFFER_LENGTH)));
        final int receiveBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.RECEIVE_BUFFER_LENGTH", String.valueOf(DEFAULT_RECEIVE_BUFFER_LENGTH)));
        final int bufferSliceLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.BUFFER_SLICE_LENGTH", String.valueOf(DEFAULT_BUFFER_SLICE_LENGTH)));
        final int flushIntervalSize = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.FLUSH_INTERVAL_SIZE", String.valueOf(DEFAULT_FLUSH_INTERVAL_SIZE)));
        final int busyPollTimeoutNanos = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.BUSY_POLL_TIMEOUT_NANOS", String.valueOf(DEFAULT_BUSY_POLL_TIMEOUT_NANOS)));
        final var pollMethod = PollMethod.valueOf(System.getProperty("de.hhu.bsinfo.hadronio.Configuration.POLL_METHOD", DEFAULT_POLL_METHOD));
        final var providerClass = System.getProperty("de.hhu.bsinfo.hadronio.Configuration.PROVIDER_CLASS", getDefaultProviderClass());

        checkConfiguration(sendBufferLength, receiveBufferLength, bufferSliceLength, flushIntervalSize, busyPollTimeoutNanos, providerClass);
        return new Configuration(sendBufferLength, receiveBufferLength, bufferSliceLength + MessageUtil.HEADER_LENGTH, flushIntervalSize, busyPollTimeoutNanos, pollMethod, providerClass);
    }

    private static void checkConfiguration(final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength, final int flushIntervalSize, final int busyPollTimeout, final String providerClass) throws IllegalArgumentException {
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

        if (busyPollTimeout <= 0) {
            throw new IllegalArgumentException("BUSY_POLL_TIMEOUT must be greater than 0!");
        }

        try {
            Class.forName(providerClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class '" + providerClass + "' does not exist!");
        }
    }

    private Configuration(final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength, final int flushIntervalSize, final int busyPollTimeoutNanos, final PollMethod pollMethod, final String providerClass) {
        this.sendBufferLength = sendBufferLength;
        this.receiveBufferLength = receiveBufferLength;
        this.bufferSliceLength = bufferSliceLength;
        this.flushIntervalSize = flushIntervalSize;
        this.busyPollTimeoutNanos = busyPollTimeoutNanos;
        this.pollMethod = pollMethod;
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

    int getBusyPollTimeoutNanos() {
        return busyPollTimeoutNanos;
    }

    PollMethod getPollMethod() {
        return pollMethod;
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
                (pollMethod == PollMethod.DYNAMIC ? (",busyPollTimeoutNanos=" + busyPollTimeoutNanos) : "") +
                ",pollMethod=" + pollMethod +
                ",providerClass=" + providerClass +
                ")";
    }
}
