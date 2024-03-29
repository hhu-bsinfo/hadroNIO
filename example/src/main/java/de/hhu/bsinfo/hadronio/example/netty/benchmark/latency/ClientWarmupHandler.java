package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWarmupHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientWarmupHandler.class);

    private final int messageSize;
    private final int messageCount;
    private final int warmupCount;

    private int receivedMessages = 0;
    private int receivedBytes = 0;

    private ByteBuf sendBuffer;

    public ClientWarmupHandler(final int messageSize, final int messageCount, final int warmupCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.warmupCount = warmupCount > 0 ? warmupCount : 1;
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
        sendBuffer = context.alloc().buffer(messageSize).retain(warmupCount + messageCount);
        for (int i = 0; i < messageSize; i++) {
            sendBuffer.writeByte(i);
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf receiveBuffer = (ByteBuf) message;
        receivedBytes += receiveBuffer.readableBytes();
        receiveBuffer.release();
        if (receivedBytes == messageSize) {
            receivedBytes = 0;
            receivedMessages++;

            sendBuffer.resetReaderIndex();
            context.channel().writeAndFlush(sendBuffer);
        }

        if (receivedMessages >= warmupCount) {
            LOGGER.info("Finished warmup");
            final ClientHandler handler = new ClientHandler(messageSize, messageCount, sendBuffer);
            context.channel().pipeline().removeLast();
            context.channel().pipeline().addLast(handler);
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
