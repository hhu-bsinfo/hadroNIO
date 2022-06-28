package de.hhu.bsinfo.hadronio.example.grpc;

import de.hhu.bsinfo.hadronio.example.grpc.benchmark.BenchmarkDemo;
import de.hhu.bsinfo.hadronio.example.grpc.echo.EchoDemo;
import de.hhu.bsinfo.hadronio.example.grpc.kvs.KeyValueStoreDemo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "grpc",
        description = "Example applications using gRPC",
        subcommands = { BenchmarkDemo.class, EchoDemo.class, KeyValueStoreDemo.class }
)
public class Grpc implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
