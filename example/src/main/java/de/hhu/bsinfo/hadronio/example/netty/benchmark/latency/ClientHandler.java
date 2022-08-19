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

    private final ByteBuf sendBuffer;

    public ClientHandler(final int messageSize, final int messageCount, final ByteBuf sendBuffer) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.sendBuffer = sendBuffer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final var receiveBuffer = (ByteBuf) message;
        receivedBytes += receiveBuffer.readableBytes();
        receiveBuffer.release();

        if (receivedBytes == messageSize) {
            receivedBytes = 0;
            receivedMessages++;

            sendBuffer.resetReaderIndex();
            context.channel().writeAndFlush(sendBuffer);
        }

        if (receivedMessages >= messageCount) {
            LOGGER.info("Finished benchmark");
            context.channel().close();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
