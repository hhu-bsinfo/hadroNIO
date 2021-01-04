package de.hhu.bsinfo.ucx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
        name = "example",
        description = "",
        showDefaultValues = true,
        separator = " ")
public class Application implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    @Override
    public void run() {

    }

    public static void main(String... args) {
        UcxProvider.printBanner();

        CommandLine cli = new CommandLine(new Application());
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        int exitCode = cli.execute(args);

        System.exit(exitCode);
    }
}
