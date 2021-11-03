package de.hhu.bsinfo.hadronio.example.netty.echo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        if (context.channel().parent() != null) {
            LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
        } else {
            LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress());
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        final ByteBuf buffer = (ByteBuf) message;
        final StringBuilder string = new StringBuilder();

        while (buffer.isReadable()) {
            string.append((char) buffer.readByte());
        }

        buffer.release();
        LOGGER.info("Received message: [{}]", string);

        if (context.channel().parent() != null) {
            context.writeAndFlush(Unpooled.copiedBuffer(string, StandardCharsets.UTF_8)).addListener(future -> LOGGER.info("Echo reply sent"));
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
