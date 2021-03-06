package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.benchmark.Benchmark;
import de.hhu.bsinfo.hadronio.benchmark.throughput.ThroughputBenchmark;
import de.hhu.bsinfo.hadronio.counter.CounterDemo;
import de.hhu.bsinfo.hadronio.util.InetSocketAddressConverter;
import picocli.CommandLine;

import java.net.InetSocketAddress;

@CommandLine.Command(
        name = "hadronio",
        description = "Test applications for hadroNIO",
        subcommands = { CounterDemo.class, Benchmark.class}
)
public class Application implements Runnable {

    private static final int DEFAULT_SERVER_PORT = 2998;

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }

    public static void main(String... args) {
        final int exitCode = new CommandLine(new Application())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(DEFAULT_SERVER_PORT))
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);

        System.exit(exitCode);
    }
}
