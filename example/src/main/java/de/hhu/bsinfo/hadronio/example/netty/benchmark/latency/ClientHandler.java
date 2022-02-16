package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);

    private final int messageSize;
    private final int messageCount;

    private int receivedMessages = 0;
    private int receivedBytes = 0;

    private ByteBuf sendBuffer;

    public ClientHandler(final int messageSize, final int messageCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
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
        LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf receiveBuffer = (ByteBuf) message;

        receivedBytes += receiveBuffer.readableBytes();
        if (receivedBytes == messageSize) {
            receivedBytes = 0;
            receivedMessages++;

            sendBuffer.resetReaderIndex();
            context.channel().writeAndFlush(sendBuffer);
        }
        receiveBuffer.release();

        if (receivedMessages >= messageCount && context.channel().parent() != null) {
            context.channel().close();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
