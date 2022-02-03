package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;
    private final int messageSize;
    private final int messageCount;

    public Server(final InetSocketAddress bindAddress, int messageSize, int messageCount) {
        this.bindAddress = bindAddress;
        this.messageSize = messageSize;
        this.messageCount = messageCount;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on [{}]", bindAddress);
        final EventLoopGroup acceptorGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        final ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
            new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    channel.closeFuture().addListener(future -> LOGGER.info("Socket channel closed"));
                    channel.pipeline().addLast(new Handler(messageSize, messageCount));
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
            e.printStackTrace();
        } finally {
            acceptorGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
