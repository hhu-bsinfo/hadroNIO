package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import de.hhu.bsinfo.hadronio.util.InetSocketAddressConverter;
import site.ycsb.Client;
import site.ycsb.workloads.CoreWorkload;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class YcsbProperties {

    public static final String REMOTE_ADDRESSES_PROPERTY = "de.hhu.bsinfo.hadronio.example.grpc.kvs.REMOTE_ADDRESSES";

    private final int fieldsPerKey;
    private final int fieldSize;
    private final int operationCount;
    private final int fieldLength;
    private final InetSocketAddress[] remoteAddresses;
    static YcsbRunner.Phase phase;
    static AtomicInteger closeConnectionCounter;

    YcsbProperties(final Properties properties) {
        fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        fieldSize = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));
        operationCount = Integer.parseInt(properties.getProperty(Client.OPERATION_COUNT_PROPERTY, "0"));
        fieldLength = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

        final var addresses = properties.getProperty(REMOTE_ADDRESSES_PROPERTY);
        final var splitAddresses = addresses.split(",");
        remoteAddresses = new InetSocketAddress[splitAddresses.length];

        final var converter = new InetSocketAddressConverter(2998);
        for (int i = 0; i < splitAddresses.length; i++) {
            remoteAddresses[i] = converter.convert(splitAddresses[i]);
        }
    }

    public int getFieldsPerKey() {
        return fieldsPerKey;
    }

    public int getFieldSize() {
        return fieldSize;
    }

    public int getOperationCount() {
        return operationCount;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public InetSocketAddress[] getRemoteAddresses() {
        return remoteAddresses;
    }
}
