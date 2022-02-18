package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static final String SYNC_SIGNAL = "SYNC";
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final Object syncLock;

    public ServerHandler(Object syncLock) {
        this.syncLock = syncLock;
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf buffer = (ByteBuf) message;
        final String signal = String.valueOf(buffer.readCharSequence(SYNC_SIGNAL.length(), StandardCharsets.UTF_8));
        if (signal.equals(SYNC_SIGNAL)) {
            synchronized (syncLock) {
                syncLock.notify();
            }
        } else {
            throw new IllegalStateException("Received wrong sync signal (Expected: '" + SYNC_SIGNAL + "', Got: '" + signal + "')!");
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
