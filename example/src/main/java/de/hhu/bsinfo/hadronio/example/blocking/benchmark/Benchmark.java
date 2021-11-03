package de.hhu.bsinfo.hadronio.example.blocking.benchmark;

import de.hhu.bsinfo.hadronio.example.blocking.benchmark.latency.LatencyBenchmark;
import de.hhu.bsinfo.hadronio.example.blocking.benchmark.throughput.ThroughputBenchmark;
import picocli.CommandLine;

@CommandLine.Command(
        name = "benchmark",
        description = "Benchmarks using blocking socket channels",
        subcommands = { ThroughputBenchmark.class, LatencyBenchmark.class }
)
public class Benchmark implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
