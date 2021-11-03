package de.hhu.bsinfo.hadronio.example;

import de.hhu.bsinfo.hadronio.example.blocking.Blocking;
import de.hhu.bsinfo.hadronio.example.netty.Netty;
import de.hhu.bsinfo.hadronio.util.InetSocketAddressConverter;
import picocli.CommandLine;

import java.net.InetSocketAddress;

@CommandLine.Command(
        name = "hadronio",
        description = "Test applications for hadroNIO",
        subcommands = { Blocking.class, Netty.class }
)
public class Application implements Runnable {

    private static final int DEFAULT_SERVER_PORT = 2998;

    @Override
    public void run() {
        CommandLine.usage(this, System.err);
    }

    public static void main(String... args) {
        System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.HadronioProvider");

        final int exitCode = new CommandLine(new Application())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(DEFAULT_SERVER_PORT))
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);

        System.exit(exitCode);
    }
}
