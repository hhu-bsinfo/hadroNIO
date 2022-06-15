package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class Shell implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress remoteAddress;
    private final Client client = new Client();

    public Shell(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void run() {
        final Scanner scanner = new Scanner(System.in);

        client.connect(remoteAddress);
        LOGGER.info("Use 'insert', 'update', 'get' or 'delete' to operate the key-value store");

        while (scanner.hasNextLine()) {
            final String line = scanner.nextLine().trim();
            if (!line.isEmpty()) {
                parseLine(line);
            }
        }
    }

    private void parseLine(final String line) {
        final String[] split = line.split(" ");
        final String command = split[0].toLowerCase();
        switch (command) {
            case "insert": {
                if (split.length < 3) {
                    LOGGER.error("Usage: insert <key> <value>");
                    return;
                }

                final Status status = client.insert(split[1], split[2]);
                if (status.isOk()) {
                    LOGGER.info("OK");
                } else {
                    LOGGER.error("Insert failed with status [{}]", status.getCode());
                }

                break;
            }
            case "update": {
                if (split.length < 3) {
                    LOGGER.error("Usage: update <key> <value>");
                    return;
                }

                final Status status = client.update(split[1], split[2]);
                if (status.isOk()) {
                    LOGGER.info("OK");
                } else {
                    LOGGER.error("Update failed with status [{}]", status.getCode());
                }

                break;
            }
            case "get": {
                if (split.length < 2) {
                    LOGGER.error("Usage: get <key>");
                    return;
                }

                final String value = client.get(split[1]);
                if (value == null) {
                    LOGGER.error("Get failed with status [NOT_FOUND]");
                } else {
                    LOGGER.info(value);
                }

                break;
            }
            case "delete": {
                if (split.length < 2) {
                    LOGGER.error("Usage: delete <key>");
                    return;
                }

                final Status status = client.delete(split[1]);
                if (status.isOk()) {
                    LOGGER.info("OK");
                } else {
                    LOGGER.error("Delete failed with status [{}]", status.getCode());
                }

                break;
            }
            default:
                LOGGER.error("Invalid command! Use 'insert', 'update', 'get' or 'delete'");
        }
    }
}
