package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import de.hhu.bsinfo.hadronio.util.ThroughputCombiner;
import de.hhu.bsinfo.hadronio.util.ThroughputResult;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class NonBlockingRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingRunnable.class);

    final BenchmarkGrpc.BenchmarkFutureStub futureStub;
    final ThroughputResult result;
    private final CyclicBarrier benchmarkBarrier;
    private final ThroughputCombiner combiner;
    private final BenchmarkMessage message;
    private final int requestCount;
    private final int aggregationTreshold;
    private final Queue<ListenableFuture<BenchmarkMessage>> futureQueue;

    public NonBlockingRunnable(final Channel channel, final CyclicBarrier syncBarrier, final ThroughputCombiner combiner, final int requestCount, final int requestSize, final int answerSize, int aggregationTreshold) {
        futureStub = BenchmarkGrpc.newFutureStub(channel);
        result = new ThroughputResult(requestCount, requestSize + answerSize);
        this.benchmarkBarrier = syncBarrier;
        this.combiner = combiner;
        this.requestCount = requestCount;
        this.aggregationTreshold = aggregationTreshold;
        futureQueue = new ArrayBlockingQueue<>(aggregationTreshold);

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
        performCalls(warmupCount);

        ListenableFuture<BenchmarkMessage> finalFuture = futureStub.benchmark(message);
        while (!finalFuture.isDone()) {
            if (finalFuture.isCancelled()) {
                throw new IllegalStateException("Warmup failed!");
            }
        }

        LOGGER.info("Finished warmup");

        try {
            // Benchmark
            benchmarkBarrier.await();
            LOGGER.info("Starting benchmark with [{}] requests", requestCount);

            final long startTime = System.nanoTime();
            performCalls(requestCount);
            result.setMeasuredTime(System.nanoTime() - startTime);

            final ManagedChannel channel = (ManagedChannel) futureStub.getChannel();
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new IllegalStateException("Benchmark failed!", e);
        }

        combiner.addResult(result);
        LOGGER.info("{}", result);
    }

    private void performCalls(final int operationCount) {
        for (int i = 0; i < aggregationTreshold; i++) {
            futureQueue.add(futureStub.benchmark(message));
        }

        int performedOperations = aggregationTreshold;
        while (performedOperations < operationCount) {
            while (futureQueue.size() > 0 && futureQueue.peek().isDone()) {
                futureQueue.poll();
            }

            while (futureQueue.size() < aggregationTreshold && performedOperations < operationCount) {
                futureQueue.add(futureStub.benchmark(message));
                performedOperations++;
            }
        }

        while (futureQueue.size() > 0) {
            ListenableFuture<BenchmarkMessage> future = futureQueue.poll();
            while (!future.isDone()) {
                if (future.isCancelled()) {
                    throw new IllegalStateException("Benchmark failed!");
                }
            }
        }
    }
}
