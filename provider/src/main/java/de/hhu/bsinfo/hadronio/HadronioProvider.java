package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.generated.BuildConfig;
import org.agrona.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.stream.Collectors;

public class HadronioProvider extends SelectorProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioProvider.class);

    private static final String DEFAULT_PROVIDER_CLASS = "de.hhu.bsinfo.hadronio.jucx.JucxProvider";

    private static final int MIN_SEND_BUFFER_LENGTH = 128;
    private static final int MIN_RECEIVE_BUFFER_LENGTH = 128;
    private static final int MIN_BUFFER_SLICE_LENGTH = 32;
    private static final int MIN_FLUSH_INTERVAL_SIZE = 128;

    private static final int DEFAULT_SEND_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_RECEIVE_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_BUFFER_SLICE_LENGTH = 32 * 1024;
    private static final int DEFAULT_FLUSH_INTERVAL_SIZE = 1024;

    private final int sendBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.SEND_BUFFER_LENGTH", String.valueOf(DEFAULT_SEND_BUFFER_LENGTH)));
    private final int receiveBufferLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.RECEIVE_BUFFER_LENGTH", String.valueOf(DEFAULT_RECEIVE_BUFFER_LENGTH)));
    private final int bufferSliceLength = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.BUFFER_SLICE_LENGTH", String.valueOf(DEFAULT_BUFFER_SLICE_LENGTH)));
    private final int flushIntervalSize = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.FLUSH_INTERVAL_SIZE", String.valueOf(DEFAULT_FLUSH_INTERVAL_SIZE)));

    private final UcxProvider provider;

    public HadronioProvider() {
        checkConfiguration();

        final String providerClass = System.getProperty("de.hhu.bsinfo.hadronio.PROVIDER_CLASS", DEFAULT_PROVIDER_CLASS);
        LOGGER.info("Using [{}] as provider implementation", providerClass);

        try {
            provider = (UcxProvider) Class.forName(providerClass).getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to instantiate class '" + providerClass + "'!", e);
        }
    }

    private void checkConfiguration() {
        if (System.getProperty("java.nio.channels.spi.SelectorProvider").equals("de.hhu.bsinfo.hadronio.HadronioProvider")) {
            LOGGER.info("de.hhu.bsinfo.hadronio.HadronioProvider is set as default SelectorProvider -> hadroNIO is active");
        } else {
            throw new IllegalStateException("de.hhu.bsinfo.hadronio.HadronioProvider is not set as default SelectorProvider -> hadroNIO is not active");
        }

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

    @Override
    public DatagramChannel openDatagramChannel() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public DatagramChannel openDatagramChannel(ProtocolFamily protocolFamily) throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public Pipe openPipe() throws IOException {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public AbstractSelector openSelector() throws IOException {
        LOGGER.info("Creating new UcxSelector");

        return new HadronioSelector(this);
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        LOGGER.info("Creating new UcxServerSocketChannel");

        final UcxServerSocketChannel serverSocketChannel = provider.createServerSocketChannel();
        return new HadronioServerSocketChannel(this, serverSocketChannel, sendBufferLength, receiveBufferLength, bufferSliceLength + HadronioSocketChannel.HEADER_LENGTH, flushIntervalSize);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        LOGGER.info("Creating new UcxSocketChannel");

        final UcxSocketChannel socketChannel = provider.createSocketChannel();
        return new HadronioSocketChannel(this, socketChannel, sendBufferLength, receiveBufferLength, bufferSliceLength + HadronioSocketChannel.HEADER_LENGTH, flushIntervalSize);
    }

    public static void printBanner() {
        final InputStream inputStream = HadronioProvider.class.getClassLoader().getResourceAsStream("banner.txt");

        if (inputStream == null) {
            return;
        }

        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        final String banner = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        System.out.print("\n");
        System.out.printf(banner, BuildConfig.VERSION, BuildConfig.BUILD_DATE, BuildConfig.GIT_BRANCH, BuildConfig.GIT_COMMIT);
        System.out.print("\n\n");
    }
}
