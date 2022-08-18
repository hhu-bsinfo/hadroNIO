package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ServerWarmupHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerWarmupHandler.class);

    private final int messageSize;
    private final int messageCount;
    private final int warmupCount;
    private final int connections;
    private final AtomicInteger warmupCounter;
    private final AtomicInteger benchmarkCounter;
    private final LatencyCombiner combiner;
    private ByteBuf sendBuffer;

    private int receivedMessages = 0;
    private int receivedBytes = 0;


    public ServerWarmupHandler(final int messageSize, final int messageCount, final int warmupCount, final int connections, final AtomicInteger warmupCounter, final AtomicInteger benchmarkCounter, final LatencyCombiner combiner) {
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.warmupCount = warmupCount > 0 ? warmupCount : 1;
        this.connections = connections;
        this.warmupCounter = warmupCounter;
        this.benchmarkCounter = benchmarkCounter;
        this.combiner = combiner;
    }

    public void start(final ChannelHandlerContext context) {
        LOGGER.info("Starting warmup with [{}] messages", warmupCount);
        sendBuffer = context.alloc().buffer(messageSize).retain(warmupCount + messageCount);
        for (int i = 0; i < messageSize; i++) {
            sendBuffer.writeByte(i);
        }
        context.channel().writeAndFlush(sendBuffer);
    }

    @Override
    public void channelActive(final ChannelHandlerContext context) {
        LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress());
    }

    @Override
    public void channelRead(final @NotNull ChannelHandlerContext context, final @NotNull Object message) {
        final var receiveBuffer = (ByteBuf) message;
        receivedBytes += receiveBuffer.readableBytes();
        receiveBuffer.release();

        if (receivedBytes == messageSize) {
            sendBuffer.resetReaderIndex();
            receivedBytes = 0;
            receivedMessages++;

            if (receivedMessages < warmupCount) {
                context.channel().writeAndFlush(sendBuffer);
            } else {
                LOGGER.info("Finished warmup");
                final ServerHandler handler = new ServerHandler(messageSize, messageCount, connections, sendBuffer, benchmarkCounter, combiner);
                context.channel().pipeline().removeLast();
                context.channel().pipeline().addLast(handler);

                if (warmupCounter.incrementAndGet() >= connections) {
                    synchronized (warmupCounter) {
                        warmupCounter.notify();
                    }
                }
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        LOGGER.error("An exception occurred", cause);
        context.channel().close();
    }
}
