package de.hhu.bsinfo.hadronio.benchmark.latency;

public interface Handler extends Runnable {

    boolean isFinished();
    void reset();
}
