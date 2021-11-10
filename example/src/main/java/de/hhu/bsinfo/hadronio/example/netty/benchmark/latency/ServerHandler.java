package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.LatencyResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final int messageSize;
    private final int messageCount;
    private final LatencyResult result;

    private int receivedMessages = 0;
    private int receivedBytes = 0;
    private long startTime;

    private ByteBuf sendBuffer;

    public ServerHandler(final int messageSize, final int messageCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        result = new LatencyResult(messageCount, messageSize);
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext context) {
        sendBuffer = context.alloc().buffer(messageSize).retain(messageCount);
        for (int i = 0; i < messageSize; i++) {
            sendBuffer.writeByte(i);
        }
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
        startTime = System.nanoTime();

        result.startSingleMeasurement();
        context.channel().writeAndFlush(sendBuffer);
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf receiveBuffer = (ByteBuf) message;

        receivedBytes += receiveBuffer.readableBytes();
        if (receivedBytes == messageSize) {
            result.stopSingleMeasurement();
            receivedBytes = 0;
            receivedMessages++;

            result.startSingleMeasurement();
            sendBuffer.resetReaderIndex();
            context.channel().writeAndFlush(sendBuffer);
        }
        receiveBuffer.release();

        if (receivedMessages >= messageCount) {
            result.finishMeasuring(System.nanoTime() - startTime);
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
