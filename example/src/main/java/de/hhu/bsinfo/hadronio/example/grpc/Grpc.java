package de.hhu.bsinfo.hadronio.example.grpc;

import de.hhu.bsinfo.hadronio.example.grpc.echo.EchoDemo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "grpc",
        description = "Example applications using gRPC",
        subcommands = { EchoDemo.class }
)
public class Grpc implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }
}
