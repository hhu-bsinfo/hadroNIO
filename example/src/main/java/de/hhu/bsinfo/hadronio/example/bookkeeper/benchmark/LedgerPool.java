package de.hhu.bsinfo.hadronio.example.bookkeeper.benchmark;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class LedgerPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(LedgerPool.class);

    private final BookKeeper client;
    private final LedgerHandle[] ledgerHandles;

    LedgerPool(final BookKeeper client, final int ledgerCount) {
        this.client = client;
        ledgerHandles = new LedgerHandle[ledgerCount];
    }

    void createLedgers() throws BKException, InterruptedException {
        LOGGER.info("Creating [{}] ledgers", ledgerHandles.length);
        for (int i = 0; i < ledgerHandles.length; i++) {
            ledgerHandles[i] = client.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32, "hadroNIO".getBytes(StandardCharsets.UTF_8));
        }
    }

    LedgerHandle getRandomLedger() {
        final var index = (int) (Math.random() * ledgerHandles.length);
        return ledgerHandles[index];
    }
}
