package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final String SYNC_SIGNAL = "SYNC";
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);

    private final int messageSize;
    private final int messageCount;
    private final int originalMessageCount;
    private final boolean warmup;

    private int receivedMessages = 0;
    private int receivedBytes = 0;

    public ClientHandler(final int messageSize, final int messageCount) {
        this.messageSize = messageSize;
        this.messageCount = messageCount / 10 > 0 ? messageCount / 10 : 1;
        originalMessageCount = messageCount;
        warmup = true;
    }

    private ClientHandler(final int messageSize, final int messageCount, final boolean warmup) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.warmup = warmup;
        originalMessageCount = messageCount;
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
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
            try {
                final ByteBuf syncBuf = context.alloc().buffer();
                syncBuf.writeCharSequence(SYNC_SIGNAL, StandardCharsets.UTF_8);
                context.channel().writeAndFlush(syncBuf).sync();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to send sync signal", e);
                context.channel().close();
            }

            if (warmup) {
                LOGGER.info("Finished warmup");
                context.channel().pipeline().removeLast();
                context.channel().pipeline().addLast(new ClientHandler(messageSize, originalMessageCount, false));
            } else {
                LOGGER.info("Finished benchmark");
                context.channel().close();
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred (receivedMessages: [{}])", receivedMessages, cause);
        context.channel().close();
    }
}
