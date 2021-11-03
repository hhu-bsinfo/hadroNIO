package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    private final int messageSize;
    private final int messageCount;
    private final ThroughputResult result;

    private int receivedMessages = 0;
    private int receivedBytes = 0;
    private long startTime;


    public Handler(final int messageSize, final int messageCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        result = new ThroughputResult(messageCount, messageSize);
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        if (context.channel().parent() != null) {
            LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
        } else {
            LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
        }

        startTime = System.nanoTime();
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

        if (receivedMessages >= messageCount) {
            result.setMeasuredTime(System.nanoTime() - startTime);
            LOGGER.info(result.toString());
            context.channel().close();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
