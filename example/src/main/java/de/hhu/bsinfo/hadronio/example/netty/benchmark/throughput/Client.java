package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress bindAddress;
    private final InetSocketAddress remoteAddress;
    private final int messageSize;
    private final int messageCount;
    private final int aggregationThreshold;

    public Client(final InetSocketAddress bindAddress, final InetSocketAddress remoteAddress, int messageSize, int messageCount, int aggregationThreshold) {
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.aggregationThreshold = aggregationThreshold;
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
                    channel.pipeline().addLast(new Handler(messageSize, messageCount));
                }
            });

        try {
            final Channel channel = bootstrap.connect(remoteAddress, bindAddress).sync().channel();
            channel.closeFuture().addListener(future -> LOGGER.info("Socket channel closed"));

            final ByteBuf[] buffers = new ByteBuf[aggregationThreshold];
            for (int i = 0; i < aggregationThreshold; i++) {
                buffers[i] = channel.alloc().buffer(messageSize).retain(messageCount / aggregationThreshold);
            }

            for (int i = 0; i < messageCount - 1; i++) {
                final ByteBuf buffer = buffers[i % aggregationThreshold];
                buffer.setIndex(0, buffer.capacity());

                if (i % aggregationThreshold == 0) {
                    channel.writeAndFlush(buffer).sync();
                } else {
                    channel.write(buffer);
                }
            }

            final ByteBuf buffer = buffers[messageCount % aggregationThreshold];
            buffer.setIndex(0, buffer.capacity());
            channel.writeAndFlush(buffer).sync();

            for (final ByteBuf buf : buffers) {
                if (buf.refCnt() > 0) {
                    buf.release(buf.refCnt());
                }
            }

            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
