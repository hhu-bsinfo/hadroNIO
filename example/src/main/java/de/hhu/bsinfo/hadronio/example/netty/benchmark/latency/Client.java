package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

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

    private static final int WORKER_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_WORKER_THREADS", "0"));
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress bindAddress;
    private final InetSocketAddress remoteAddress;
    private final int messageSize;
    private final int messageCount;
    private final int connections;

    private final Channel[] channels;

    public Client(final InetSocketAddress bindAddress, final InetSocketAddress remoteAddress, final int messageSize, final int messageCount, final int connections) {
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.connections = connections;
        channels = new Channel[connections];
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final var bootstrap = new Bootstrap();

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel channel) {
                        channel.pipeline().addLast(new ClientWarmupHandler(messageSize, messageCount, messageCount / 10));
                    }
                });

        try {
            for (int i = 0; i < connections; i++) {
                final var channel = bootstrap.connect(remoteAddress, bindAddress).sync().channel();
                channel.closeFuture().addListener(future -> LOGGER.info("Closed channel connected to [{}]", channel.remoteAddress()));
                channels[i] = channel;
            }

            for (int i = 0; i < connections; i++) {
                channels[i].closeFuture().sync();
            }
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
