package de.hhu.bsinfo.hadronio.example.netty;

import de.hhu.bsinfo.hadronio.example.netty.benchmark.Benchmark;
import de.hhu.bsinfo.hadronio.example.netty.echo.EchoDemo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "netty",
        description = "Example applications using netty",
        subcommands = { EchoDemo.class, Benchmark.class }
)
public class Netty implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
