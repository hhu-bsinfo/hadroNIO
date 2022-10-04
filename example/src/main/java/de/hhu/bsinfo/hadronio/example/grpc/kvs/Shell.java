package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class Shell implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress[] remoteAddresses;
    private final Client client = new Client();

    public Shell(final InetSocketAddress[] remoteAddresses) {
        this.remoteAddresses = remoteAddresses;
    }

    @Override
    public void run() {
        final Scanner scanner = new Scanner(System.in);

        client.connect(remoteAddresses);
        LOGGER.info("Use 'insert', 'update', 'get' or 'delete' to operate the key-value store");

        while (scanner.hasNextLine()) {
            final var line = scanner.nextLine().trim();
            if (!line.isEmpty()) {
                parseLine(line);
            }
        }
    }

    private void parseLine(final String line) {
        Status status;
        String value = null;
        final var split = line.split(" ");
        final var command = split[0].toLowerCase();
        final long startTime = System.nanoTime();

        switch (command) {
            case "insert": {
                if (split.length < 3) {
                    LOGGER.error("Usage: insert <key> <value>");
                    return;
                }

                status = client.insert(split[1], split[2]);
                break;
            }
            case "update": {
                if (split.length < 3) {
                    LOGGER.error("Usage: update <key> <value>");
                    return;
                }

                status = client.update(split[1], split[2]);
                break;
            }
            case "get": {
                if (split.length < 2) {
                    LOGGER.error("Usage: get <key>");
                    return;
                }

                value = client.get(split[1]);
                status = value == null ? Status.NOT_FOUND : Status.OK;
                break;
            }
            case "delete": {
                if (split.length < 2) {
                    LOGGER.error("Usage: delete <key>");
                    return;
                }

                status = client.delete(split[1]);
                break;
            }
            default:
                LOGGER.error("Invalid command! Use 'insert', 'update', 'get' or 'delete'");
                return;
        }

        final double time = (System.nanoTime() - startTime) / (double) 1000000;
        if (status.isOk()) {
            LOGGER.info("{} ({} ms)", value == null ? status.getCode() : value, String.format("%.03f", time));
        } else {
            LOGGER.error("{} failed with status [{}]", Character.toUpperCase(command.charAt(0)) + command.substring(1), status.getCode());
        }
    }
}
