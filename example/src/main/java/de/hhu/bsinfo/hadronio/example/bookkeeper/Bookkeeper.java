package de.hhu.bsinfo.hadronio.example.bookkeeper;

import de.hhu.bsinfo.hadronio.example.bookkeeper.benchmark.Benchmark;
import picocli.CommandLine;

@CommandLine.Command(
        name = "bookkeeper",
        description = "Example applications using bookkeeper",
        subcommands = { Benchmark.class }
)
public class Bookkeeper implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}