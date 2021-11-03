package de.hhu.bsinfo.hadronio.example.netty.hello;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
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

    public Client(final InetSocketAddress bindAddress, final InetSocketAddress remoteAddress) {
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        final Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(final SocketChannel channel) {
                    channel.pipeline().addLast(new Handler("Hello from Client!"));
                }
            });

        try {
            bootstrap.connect(remoteAddress, bindAddress).sync()
                .channel().closeFuture().addListener(future -> LOGGER.info("Socket channel closed")).sync();
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
