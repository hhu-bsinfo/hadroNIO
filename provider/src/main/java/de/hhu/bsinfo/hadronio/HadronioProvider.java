package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.generated.BuildConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.stream.Collectors;

public class HadronioProvider extends SelectorProvider implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioProvider.class);

    private final UcxProvider provider;

    public HadronioProvider() {
        if (System.getProperty("java.nio.channels.spi.SelectorProvider").equals("de.hhu.bsinfo.hadronio.HadronioProvider")) {
            LOGGER.info("de.hhu.bsinfo.hadronio.HadronioProvider is set as default SelectorProvider -> hadroNIO is active");
        } else {
            throw new IllegalStateException("de.hhu.bsinfo.hadronio.HadronioProvider is not set as default SelectorProvider -> hadroNIO is not active");
        }

        final Configuration configuration = Configuration.getInstance();
        LOGGER.info("hadroNIO configuration: [{}]", configuration);

        try {
            provider = (UcxProvider) Class.forName(configuration.getProviderClass()).getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to instantiate class '" + configuration.getProviderClass() + "'!", e);
        }
    }

    @Override
    public DatagramChannel openDatagramChannel() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public DatagramChannel openDatagramChannel(ProtocolFamily protocolFamily) {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public Pipe openPipe() {
        throw new UnsupportedOperationException("Operation not supported!");
    }

    @Override
    public AbstractSelector openSelector() {
        LOGGER.info("Creating new HadronioSelector");

        return new HadronioSelector(this);
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() {
        LOGGER.info("Creating new HadronioServerSocketChannel");

        final UcxServerSocketChannel serverSocketChannel = provider.createServerSocketChannel();
        return new HadronioServerSocketChannel(this, serverSocketChannel);
    }

    @Override
    public SocketChannel openSocketChannel() {
        LOGGER.info("Creating new HadronioSocketChannel");

        final UcxSocketChannel socketChannel = provider.createSocketChannel();
        return new HadronioSocketChannel(this, socketChannel);
    }

    public static void printBanner() {
        final InputStream inputStream = HadronioProvider.class.getClassLoader().getResourceAsStream("banner.txt");

        if (inputStream == null) {
            return;
        }

        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        final String banner = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        System.out.print("\n");
        System.out.printf(banner, BuildConfig.VERSION, BuildConfig.BUILD_DATE, BuildConfig.GIT_BRANCH, BuildConfig.GIT_COMMIT, Configuration.getInstance().getProviderClass());
        System.out.print("\n\n");
    }

    @Override
    public void close() throws IOException {
        provider.close();
    }
}
