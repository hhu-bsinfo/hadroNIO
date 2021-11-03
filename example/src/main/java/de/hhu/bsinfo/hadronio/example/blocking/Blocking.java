package de.hhu.bsinfo.hadronio.example.blocking;

import de.hhu.bsinfo.hadronio.example.blocking.benchmark.Benchmark;
import de.hhu.bsinfo.hadronio.example.blocking.counter.CounterDemo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "blocking",
        description = "Example applications using blocking socket channels",
        subcommands = { Benchmark.class, CounterDemo.class }
)
public class Blocking implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
