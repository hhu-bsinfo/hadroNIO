package de.hhu.bsinfo.hadronio.benchmark;

import de.hhu.bsinfo.hadronio.benchmark.latency.LatencyBenchmark;
import de.hhu.bsinfo.hadronio.benchmark.throughput.ThroughputBenchmark;
import picocli.CommandLine;

@CommandLine.Command(
        name = "benchmark",
        description = "Benchmarks for hadroNIO",
        subcommands = { ThroughputBenchmark.class, LatencyBenchmark.class }
)
public class Benchmark implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
