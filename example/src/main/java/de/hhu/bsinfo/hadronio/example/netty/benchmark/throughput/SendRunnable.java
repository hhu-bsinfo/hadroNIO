package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import de.hhu.bsinfo.hadronio.util.ThroughputCombiner;
import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SendRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendRunnable.class);

    private final int messageCount;
    private final int aggregationThreshold;
    private final Object syncLock;
    private final CyclicBarrier benchmarkBarrier;
    private final Channel channel;
    private final ThroughputCombiner combiner;
    private final ThroughputResult result;
    private final ByteBuf[] buffers;

    public SendRunnable(final int messageSize, final int messageCount, final int aggregationThreshold, final Object syncLock, final CyclicBarrier syncBarrier, final Channel channel, final ThroughputCombiner combiner) {
        this.messageCount = messageCount;
        this.aggregationThreshold = aggregationThreshold;
        this.syncLock = syncLock;
        this.benchmarkBarrier = syncBarrier;
        this.channel = channel;
        this.combiner = combiner;

        result = new ThroughputResult(messageCount, messageSize);
        buffers = new ByteBuf[aggregationThreshold];
        for (int i = 0; i < aggregationThreshold; i++) {
            buffers[i] = channel.alloc().buffer(messageSize);
        }
    }

    @Override
    public void run() {
        try {
            // Warmup
            final int warmupCount = messageCount / 10 > 0 ? messageCount / 10 : 1;
            LOGGER.info("Starting warmup with [{}] messages", warmupCount);

            synchronized (syncLock) {
                sendMessages(warmupCount);
                syncLock.wait();
            }

            LOGGER.info("Finished warmup");
        } catch (InterruptedException e) {
            LOGGER.error("Warmup failed", e);
            channel.close();
            return;
        }

        try {
            // Benchmark
            benchmarkBarrier.await();
            LOGGER.info("Starting benchmark with [{}] messages", messageCount);
            final long startTime = System.nanoTime();

            synchronized (syncLock) {
                sendMessages(messageCount);
                syncLock.wait();
            }

            result.setMeasuredTime(System.nanoTime() - startTime);
        } catch (InterruptedException | BrokenBarrierException e) {
            LOGGER.error("Benchmark failed", e);
            channel.close();
            return;
        }

        // Cleanup buffers
        for (final ByteBuf buf : buffers) {
            if (buf.refCnt() > 0) {
                buf.release(buf.refCnt());
            }
        }

        combiner.addResult(result);
        LOGGER.info("{}", result);

        channel.close();
    }

    private void sendMessages(int messageCount) throws InterruptedException {
        for (int i = 0; i < aggregationThreshold; i++) {
            buffers[i].retain(messageCount / aggregationThreshold + 1);
        }

        for (int i = 0; i < messageCount - 1; i++) {
            final ByteBuf buffer = buffers[i % aggregationThreshold];
            buffer.setIndex(0, buffer.capacity());

            if (i % aggregationThreshold == 0) {
                channel.writeAndFlush(buffer).sync();
            } else {
                channel.write(buffer);
            }
        }

        final ByteBuf buffer = buffers[messageCount % aggregationThreshold];
        buffer.setIndex(0, buffer.capacity());
        channel.writeAndFlush(buffer).sync();
    }

    public Channel getChannel() {
        return channel;
    }
}
