package de.hhu.bsinfo.hadronio.example.netty.benchmark.connection;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Server implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;

    public Server(final InetSocketAddress bindAddress) {
        this.bindAddress = bindAddress;
    }


    @Override
    public void run() {
        LOGGER.info("Starting server on [{}]", bindAddress);
        final var acceptorGroup = new NioEventLoopGroup();
        final var workerGroup = new NioEventLoopGroup(1);
        final var bootstrap = new ServerBootstrap();

        bootstrap.group(acceptorGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(final SocketChannel channel) {
                                channel.closeFuture().addListener(future -> LOGGER.info("Closed channel connected to [{}]", channel.remoteAddress()));
                            }
                        });

        final var serverChannel = bootstrap.bind(bindAddress).addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.info("Server is running");
            } else {
                LOGGER.error("Unable to start server", future.cause());
            }
        }).channel();
        serverChannel.closeFuture().addListener(future -> LOGGER.info("Server channel closed"));

        try {
            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
