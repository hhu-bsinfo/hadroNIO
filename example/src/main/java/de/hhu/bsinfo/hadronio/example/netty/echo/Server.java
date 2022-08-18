package de.hhu.bsinfo.hadronio.example.netty.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = 1;
    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;

    public Server(final InetSocketAddress bindAddress) {
        this.bindAddress = bindAddress;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on [{}]", bindAddress);
        final var acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final var bootstrap = new ServerBootstrap();

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(@NotNull SocketChannel channel) {
                        channel.closeFuture().addListener(future -> LOGGER.info("Socket channel closed"));
                        channel.pipeline().addLast(new Handler());
                    }
                });

        try {
            bootstrap.bind(bindAddress).addListener(future -> {
                if (future.isSuccess()) {
                    LOGGER.info("Server is running");
                } else {
                    LOGGER.error("Unable to start server", future.cause());
                }
            }).channel().closeFuture().addListener(future -> LOGGER.info("Server socket channel closed")).sync();
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
        } finally {
            workerGroup.shutdownGracefully();
            acceptorGroup.shutdownGracefully();
        }
    }
}
