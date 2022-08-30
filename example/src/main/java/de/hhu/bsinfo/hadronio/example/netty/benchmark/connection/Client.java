package de.hhu.bsinfo.hadronio.example.netty.benchmark.connection;

import de.hhu.bsinfo.hadronio.util.LatencyResult;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Client implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress bindAddress;
    private final InetSocketAddress remoteAddress;
    private final boolean keepConnectionsAlive;
    private final Channel[] channels;
    private final LatencyResult result;

    public Client(final InetSocketAddress bindAddress, final InetSocketAddress remoteAddress, boolean keepConnectionsAlive, final int connections) {
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
        this.keepConnectionsAlive = keepConnectionsAlive;
        channels = new Channel[connections];
        result = new LatencyResult(connections, 1);
    }


    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final var workerGroup = new NioEventLoopGroup(1);
        final var bootstrap = new Bootstrap();

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel channel) {}
                });

        try {
            final long startTime = System.nanoTime();
            for (int i = 0; i < channels.length; i++) {
                result.startSingleMeasurement();
                channels[i] = bootstrap.connect(remoteAddress, bindAddress).sync().channel();
                result.stopSingleMeasurement();

                if (!keepConnectionsAlive) {
                    channels[i].close().sync();
                }
            }
            result.setMeasuredTime(System.nanoTime() - startTime);

            if (keepConnectionsAlive) {
                for (final var channel : channels) {
                    channel.close().sync();
                }
            }

            LOGGER.info("{}", result);
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
