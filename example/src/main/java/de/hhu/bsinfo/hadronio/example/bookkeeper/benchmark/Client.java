package de.hhu.bsinfo.hadronio.example.bookkeeper.benchmark;

import de.hhu.bsinfo.hadronio.example.bookkeeper.Bookkeeper;
import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CyclicBarrier;

public class Client implements Runnable {

    private static final int WORKER_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_WORKER_THREADS", "0"));

    private static final Logger LOGGER = LoggerFactory.getLogger(Benchmark.class);

    private final String remoteAddress;
    private final int ledgerCount;
    private final int messageCount;
    private final int messageSize;
    private final int connections;

    private final CyclicBarrier syncBarrier;
    private final LatencyCombiner combiner = new LatencyCombiner();

    private BookKeeper[] clients;

    public Client(final String remoteAddress, final int ledgerCount, final int messageCount, final int messageSize, final int connections) {
        this.remoteAddress = remoteAddress;
        this.ledgerCount = ledgerCount;
        this.messageCount = messageCount;
        this.messageSize = messageSize;
        this.connections = connections;
        syncBarrier = new CyclicBarrier(connections);
        clients = new BookKeeper[connections];
    }

    @Override
    public void run() {
        final var pools = new LedgerPool[connections];
        final var runnables = new BenchmarkRunnable[connections];
        final var threads = new Thread[connections];

        try {
            for (int i = 0; i < connections; i++) {
                clients[i] = new BookKeeper(remoteAddress);
                pools[i] = new LedgerPool(clients[i], ledgerCount);
                runnables[i] = new BenchmarkRunnable(clients[i], pools[i], syncBarrier, combiner, messageCount, messageSize);
                threads[i] = new Thread(runnables[i]);
                LOGGER.info("Successfully connected to bookkeeper cluster");
            }
        } catch (BKException | IOException | InterruptedException e) {
            LOGGER.error("Failed to connect to bookkeeper cluster", e);
            return;
        }

        for (int i = 0; i < connections; i++) {
            threads[i].start();
        }

        for (int i = 0; i < connections; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to join thread [{}]", threads[i].toString(), e);
            }
        }

        final var result = combiner.getCombinedResult();
        LOGGER.info("{}", result);
    }
}
