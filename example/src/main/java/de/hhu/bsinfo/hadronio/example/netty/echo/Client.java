package de.hhu.bsinfo.hadronio.example.netty.echo;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress bindAddress;
    private final InetSocketAddress remoteAddress;
    private final Scanner scanner = new Scanner(System.in);

    public Client(final InetSocketAddress bindAddress, final InetSocketAddress remoteAddress) {
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
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
                    channel.pipeline().addLast(new Handler());
                }
            });

        try {
            final var channel = bootstrap.connect(remoteAddress, bindAddress).sync().channel();
            final var scanThread = new Thread(() -> {
                while (scanner.hasNextLine()) {
                    try {
                        final var line = scanner.nextLine();
                        channel.writeAndFlush(Unpooled.copiedBuffer(line, StandardCharsets.UTF_8)).sync();
                    } catch (InterruptedException e) {
                        LOGGER.error("A sync error occurred", e);
                    }
                }

                channel.close();
            });

            scanThread.start();
            channel.closeFuture().addListener(future -> LOGGER.info("Socket channel closed")).sync();
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
