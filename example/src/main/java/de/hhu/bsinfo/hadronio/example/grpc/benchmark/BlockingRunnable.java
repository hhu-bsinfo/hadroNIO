package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.protobuf.ByteString;
import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.LatencyResult;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class BlockingRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingRunnable.class);

    final BenchmarkGrpc.BenchmarkBlockingStub blockingStub;
    final LatencyResult result;
    private final CyclicBarrier benchmarkBarrier;
    private final LatencyCombiner combiner;
    private final BenchmarkMessage message;
    private final int requestCount;

    public BlockingRunnable(final Channel channel, final CyclicBarrier syncBarrier, final LatencyCombiner combiner, final int requestCount, final int requestSize, final int answerSize) {
        blockingStub = BenchmarkGrpc.newBlockingStub(channel);
        result = new LatencyResult(requestCount, requestSize + answerSize);
        this.benchmarkBarrier = syncBarrier;
        this.combiner = combiner;
        this.requestCount = requestCount;

        final byte[] requestBytes = new byte[requestSize];
        for (int i = 0; i < requestSize; i++) {
            requestBytes[i] = (byte) i;
        }
        message = BenchmarkMessage.newBuilder().setContent(ByteString.copyFrom(requestBytes)).build();
    }

    @Override
    public void run() {
        // Warmup
        final int warmupCount = requestCount / 10 > 0 ? requestCount / 10 : 1;
        LOGGER.info("Starting warmup with [{}] requests", warmupCount);

        for (int i = 0; i < warmupCount; i++) {
            blockingStub.benchmark(message);
        }

        LOGGER.info("Finished warmup");

        try {
            // Benchmark
            benchmarkBarrier.await();
            LOGGER.info("Starting benchmark with [{}] requests", requestCount);
            final long startTime = System.nanoTime();

            for (int i = 0; i < requestCount; i++) {
                result.startSingleMeasurement();
                blockingStub.benchmark(message);
                result.stopSingleMeasurement();
            }

            result.setMeasuredTime(System.nanoTime() - startTime);

            final var channel = (ManagedChannel) blockingStub.getChannel();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new IllegalStateException("Benchmark failed!", e);
        }

        combiner.addResult(result);
        LOGGER.info("{}", result);
    }
}
