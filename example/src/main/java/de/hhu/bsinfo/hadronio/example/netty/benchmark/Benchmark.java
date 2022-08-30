package de.hhu.bsinfo.hadronio.example.netty.benchmark;

import de.hhu.bsinfo.hadronio.example.netty.benchmark.connection.ConnectionBenchmark;
import de.hhu.bsinfo.hadronio.example.netty.benchmark.latency.LatencyBenchmark;
import de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput.ThroughputBenchmark;
import picocli.CommandLine;

@CommandLine.Command(
        name = "benchmark",
        description = "Benchmarks using netty",
        subcommands = { ThroughputBenchmark.class, LatencyBenchmark.class, ConnectionBenchmark.class}
)
public class Benchmark implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
