package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WarmupHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarmupHandler.class);

    private final int messageSize;
    private final int messageCount;
    private final int warmupCount;

    private int receivedMessages = 0;
    private int receivedBytes = 0;

    public WarmupHandler(final int messageSize, final int messageCount, final int warmupCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.warmupCount = warmupCount > 0 ? warmupCount : 1;
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        if (context.channel().parent() != null) {
            LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
        } else {
            LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
        }

        LOGGER.info("Starting warmup with [{}] messages", warmupCount);
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf buffer = (ByteBuf) message;

        receivedBytes += buffer.readableBytes();
        while (receivedBytes >= messageSize) {
            receivedBytes -= messageSize;
            receivedMessages++;
        }
        buffer.release();

        if (receivedMessages >= warmupCount) {
            LOGGER.info("Starting benchmark with [{}] messages", messageCount);
            context.channel().pipeline().removeLast();
            context.channel().pipeline().addLast(new Handler(messageSize, messageCount));
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
