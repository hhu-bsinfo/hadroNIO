package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.generated.BuildConfig;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
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
    private static final int MIN_RECEIVE_SLICE_LENGTH = 32;

    private static final int DEFAULT_SEND_BUFFER_LENGTH = 1048576 * 4;
    private static final int DEFAULT_RECEIVE_BUFFER_LENGTH = 1048576 * 4;
    private static final int DEFAULT_RECEIVE_SLICE_LENGTH = 1024 * 32;

    private static final int SEND_BUFFER_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.SEND_BUFFER_LENGTH", String.valueOf(DEFAULT_SEND_BUFFER_LENGTH)));
    private static final int RECEIVE_BUFFER_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.RECEIVE_BUFFER_LENGTH", String.valueOf(DEFAULT_RECEIVE_BUFFER_LENGTH)));
    private static final int RECEIVE_SLICE_LENGTH = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.RECEIVE_SLICE_LENGTH", String.valueOf(DEFAULT_RECEIVE_SLICE_LENGTH)));

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

        if (RECEIVE_SLICE_LENGTH < MIN_RECEIVE_SLICE_LENGTH) {
            throw new IllegalArgumentException("RECEIVE_SLICE_LENGTH must be a at least " + MIN_RECEIVE_SLICE_LENGTH + " byte!");
        }

        if (RECEIVE_BUFFER_LENGTH % RECEIVE_SLICE_LENGTH != 0) {
            throw new IllegalArgumentException("RECEIVE_SLICE_LENGTH must be a restless divisor of RECEIVE_BUFFER_LENGTH!");
        }
    }

    private final UcpContext context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature().requestStreamFeature());

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

        return new UcxServerSocketChannel(this, context, SEND_BUFFER_LENGTH, RECEIVE_BUFFER_LENGTH, RECEIVE_SLICE_LENGTH);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        LOGGER.info("Creating new UcxSocketChannel");

        return new UcxSocketChannel(this, context, SEND_BUFFER_LENGTH, RECEIVE_BUFFER_LENGTH, RECEIVE_SLICE_LENGTH);
    }

    public static void printBanner() {
        InputStream inputStream = UcxProvider.class.getClassLoader().getResourceAsStream("banner.txt");

        if (inputStream == null) {
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String banner = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        System.out.print("\n");
        System.out.printf(banner, BuildConfig.VERSION, BuildConfig.BUILD_DATE, BuildConfig.GIT_BRANCH, BuildConfig.GIT_COMMIT);
        System.out.print("\n\n");
    }
}
