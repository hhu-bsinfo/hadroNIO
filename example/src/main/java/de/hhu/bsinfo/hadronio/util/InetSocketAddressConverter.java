package de.hhu.bsinfo.hadronio.util;

import java.net.InetSocketAddress;
import picocli.CommandLine;

public class InetSocketAddressConverter implements CommandLine.ITypeConverter<InetSocketAddress> {

    private final int defaultPort;

    public InetSocketAddressConverter(final int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public InetSocketAddress convert(final String address) throws Exception {

        final String[] splittedAddress = address.split(":");

        if (splittedAddress.length == 0 || splittedAddress.length > 2) {
            throw new CommandLine.TypeConversionException("Invalid connection string specified");
        }

        final String hostname = splittedAddress[0].length() == 0 ? "0.0.0.0" : splittedAddress[0];

        int port = defaultPort;
        if (splittedAddress.length > 1) {
            try {
                port = Integer.parseInt(splittedAddress[1]);
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
