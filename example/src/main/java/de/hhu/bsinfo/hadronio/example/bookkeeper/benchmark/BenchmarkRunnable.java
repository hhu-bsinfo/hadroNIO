package de.hhu.bsinfo.hadronio.example.bookkeeper.benchmark;

import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.LatencyResult;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class BenchmarkRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkRunnable.class);

    private final BookKeeper bookKeeper;
    private final LedgerPool ledgerPool;
    private final LatencyResult result;
    private final CyclicBarrier syncBarrier;
    private final LatencyCombiner combiner;
    private final int requestCount;
    private final byte[] data;

    BenchmarkRunnable(final BookKeeper bookKeeper, final LedgerPool ledgerPool, final CyclicBarrier syncBarrier, final LatencyCombiner combiner, final int requestCount, final int requestSize) {
        this.bookKeeper = bookKeeper;
        this.ledgerPool = ledgerPool;
        this.syncBarrier = syncBarrier;
        this.combiner = combiner;
        this.requestCount = requestCount;
        this.data = new byte[requestSize];
        result = new LatencyResult(requestCount, requestSize);
    }

    @Override
    public void run() {
        // Setup
        try {
            ledgerPool.createLedgers();
        } catch (BKException | InterruptedException e) {
            LOGGER.error("Failed to create ledgers", e);
            return;
        }

        // Warmup
        final int warmupCount = requestCount / 10 > 0 ? requestCount / 10 : 1;
        LOGGER.info("Starting warmup with [{}] requests", warmupCount);

        try {
            for (int i = 0; i < requestCount; i++) {
                ledgerPool.getRandomLedger().addEntry(data);
            }
        } catch (BKException | InterruptedException e) {
            LOGGER.error("Failed get data from benchmark file", e);
            return;
        }

        LOGGER.info("Finished warmup");

        // Benchmark
        try {
            syncBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Starting benchmark with [{}] requests", requestCount);
        try {
            final var startTime = System.nanoTime();

            for (int i = 0; i < requestCount; i++) {
                result.startSingleMeasurement();
                ledgerPool.getRandomLedger().addEntry(data);
                result.stopSingleMeasurement();
            }

            result.setMeasuredTime(System.nanoTime() - startTime);
        } catch (BKException | InterruptedException e) {
            LOGGER.error("Failed get data from benchmark file", e);
            return;
        }

        try {
            bookKeeper.close();
        } catch (BKException | InterruptedException e) {
            LOGGER.error("Failed to close connection to bookkeeper server");
        }

        combiner.addResult(result);
        LOGGER.info("{}", result);
    }
}
