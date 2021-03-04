package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.generated.BuildConfig;
import org.agrona.BitUtil;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.stream.Collectors;

public class UcxProvider extends SelectorProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxProvider.class);

    private static final int MIN_SEND_BUFFER_LENGTH = 128;
    private static final int MIN_RECEIVE_BUFFER_LENGTH = 128;
    private static final int MIN_BUFFER_SLICE_LENGTH = 32;

    private static final int DEFAULT_SEND_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_RECEIVE_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int DEFAULT_BUFFER_SLICE_LENGTH = 32 * 1024;

    private static final int SEND_BUFFER_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.SEND_BUFFER_LENGTH", String.valueOf(DEFAULT_SEND_BUFFER_LENGTH)));
    private static final int RECEIVE_BUFFER_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.RECEIVE_BUFFER_LENGTH", String.valueOf(DEFAULT_RECEIVE_BUFFER_LENGTH)));
    private static final int BUFFER_SLICE_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.BUFFER_SLICE_LENGTH", String.valueOf(DEFAULT_BUFFER_SLICE_LENGTH)));

    static {
        if (System.getProperty("java.nio.channels.spi.SelectorProvider").equals("de.hhu.bsinfo.hadronio.UcxProvider")) {
            LOGGER.info("UcxProvider is set as default SelectorProvider -> hadroNIO is active");
        } else {
            LOGGER.warn("UcxProvider is not set as default SelectorProvider -> hadroNIO is not active");
        }

        if (SEND_BUFFER_LENGTH < MIN_SEND_BUFFER_LENGTH) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least " + MIN_SEND_BUFFER_LENGTH + " byte!");
        }

        if (RECEIVE_BUFFER_LENGTH < MIN_RECEIVE_BUFFER_LENGTH) {
            throw new IllegalArgumentException("RECEIVE_BUFFER_LENGTH must be a at least " + MIN_RECEIVE_BUFFER_LENGTH + " byte!");
        }

        if (BUFFER_SLICE_LENGTH < MIN_BUFFER_SLICE_LENGTH) {
            throw new IllegalArgumentException("BUFFER_SLICE_LENGTH must be a at least " + MIN_BUFFER_SLICE_LENGTH + " byte!");
        }

        if (!BitUtil.isPowerOfTwo(BUFFER_SLICE_LENGTH)) {
            throw new IllegalArgumentException("BUFFER_SLICE_LENGTH must be a power of 2");
        }

        if (!BitUtil.isPowerOfTwo(SEND_BUFFER_LENGTH)) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a power of 2");
        }

        if (!BitUtil.isPowerOfTwo(RECEIVE_BUFFER_LENGTH)) {
            throw new IllegalArgumentException("RECEIVE_BUFFER_LENGTH must be a power of 2");
        }

        if (SEND_BUFFER_LENGTH < 2 * BUFFER_SLICE_LENGTH) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least twice as high as BUFFER_SLICE_LENGTH!");
        }

        if (RECEIVE_BUFFER_LENGTH < 2 * BUFFER_SLICE_LENGTH) {
            throw new IllegalArgumentException("SEND_BUFFER_LENGTH must be a at least twice as high as RECEIVE_BUFFER_LENGTH!");
        }
    }

    private final UcpContext context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());

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

        return new UcxSelector(this);
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        LOGGER.info("Creating new UcxServerSocketChannel");

        return new UcxServerSocketChannel(this, context, SEND_BUFFER_LENGTH, RECEIVE_BUFFER_LENGTH, BUFFER_SLICE_LENGTH);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        LOGGER.info("Creating new UcxSocketChannel");

        return new UcxSocketChannel(this, context, SEND_BUFFER_LENGTH, RECEIVE_BUFFER_LENGTH, BUFFER_SLICE_LENGTH);
    }

    public static void printBanner() {
        final InputStream inputStream = UcxProvider.class.getClassLoader().getResourceAsStream("banner.txt");

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
