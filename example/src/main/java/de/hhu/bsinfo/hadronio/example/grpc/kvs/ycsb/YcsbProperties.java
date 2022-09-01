package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import de.hhu.bsinfo.hadronio.util.InetSocketAddressConverter;
import site.ycsb.workloads.CoreWorkload;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class YcsbProperties {

    public static final String REMOTE_ADDRESS_PROPERTY = "de.hhu.bsinfo.hadronio.example.grpc.kvs.REMOTE_ADDRESS";

    private final int fieldsPerKey;
    private final int fieldSize;
    private final InetSocketAddress remoteAddress;
    static YcsbRunner.Phase phase;
    static AtomicInteger closeConnectionCounter;

    YcsbProperties(final Properties properties) {
        fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        fieldSize = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));
        final var remoteAddress = properties.getProperty(REMOTE_ADDRESS_PROPERTY);

        try {
            this.remoteAddress = new InetSocketAddressConverter(0).convert(remoteAddress);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public int getFieldsPerKey() {
        return fieldsPerKey;
    }

    public int getFieldSize() {
        return fieldSize;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }
}
