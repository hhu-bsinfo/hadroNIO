package de.hhu.bsinfo.hadronio.util;

import java.net.InetSocketAddress;
import picocli.CommandLine;

public class InetSocketAddressConverter implements CommandLine.ITypeConverter<InetSocketAddress> {

    private final int defaultPort;

    public InetSocketAddressConverter(final int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public InetSocketAddress convert(final String address) {

        final var splitAddress = address.split(":");

        if (splitAddress.length == 0 || splitAddress.length > 2) {
            throw new CommandLine.TypeConversionException("Invalid connection string specified");
        }

        final var hostname = splitAddress[0].length() == 0 ? "0.0.0.0" : splitAddress[0];

        int port = defaultPort;
        if (splitAddress.length > 1) {
            try {
                port = Integer.parseInt(splitAddress[1]);
            } catch (NumberFormatException e) {
                throw new CommandLine.TypeConversionException("Invalid port specified");
            }
        }

        try {
            return new InetSocketAddress(hostname, port);
        } catch (IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(e.getMessage());
        }
    }
}
