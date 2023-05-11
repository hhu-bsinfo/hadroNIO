package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import de.hhu.bsinfo.hadronio.example.grpc.kvs.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class YcsbBinding extends DB {

    private static final Logger LOGGER = LoggerFactory.getLogger(YcsbBinding.class);

    private static final String NAMESPACE_SEPARATOR = ".";

    private static AtomicLong firstInitStartTime = new AtomicLong(0);
    private static AtomicLong lastInitFinishedTime = new AtomicLong(0);
    private static AtomicLong operationCount = new AtomicLong(0);

    private final Client client = new Client();

    private YcsbProperties properties;
    private YcsbObject reusableObject;

    public static long getInitTime() {
        return lastInitFinishedTime.get() - firstInitStartTime.get();
    }

    public static long getOperationCount() {
        return operationCount.get();
    }

    @Override
    public void init() {
        firstInitStartTime.compareAndSet(0, System.currentTimeMillis());

        LOGGER.info("Initializing YCSB client");
        properties = new YcsbProperties(getProperties());
        reusableObject = new YcsbObject(properties.getFieldsPerKey(), properties.getFieldSize());
        client.connect(properties.getRemoteAddresses());
        operationCount.compareAndSet(0, properties.getOperationCount());
        CsvHistogramExporter.recordSize = properties.getFieldLength();

        if (YcsbProperties.phase == YcsbRunner.Phase.RUN) {
            client.startBenchmark();
        }

        lastInitFinishedTime.set(System.currentTimeMillis());
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        for (final var entry : values.entrySet()) {
            reusableObject.setFieldValue(entry.getKey(), entry.getValue());
        }

        final var status = client.insert(generateKey(table, key), reusableObject.getData());
        return status == io.grpc.Status.OK ? Status.OK : Status.FORBIDDEN;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        for (final var entry : values.entrySet()) {
            reusableObject.setFieldValue(entry.getKey(), entry.getValue());
        }

        final var status = client.update(generateKey(table, key), reusableObject.getData());
        return status == io.grpc.Status.OK ? Status.OK : Status.NOT_FOUND;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        final byte[] data = client.get(generateKey(table, key));
        if (data == null) {
            return Status.NOT_FOUND;
        }

        reusableObject.setData(data);

        // Read all fields
        if (fields == null) {
            for (int i = 0; i < properties.getFieldsPerKey(); i++) {
                result.put("field" + i, reusableObject.getFieldIterator(i));
            }
        } else {
            for (final var field : fields) {
                result.put(field, reusableObject.getFieldIterator(field));
            }
        }

        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        final var status = client.delete(generateKey(table, key));
        return status == io.grpc.Status.OK ? Status.OK : Status.NOT_FOUND;
    }

    @Override
    public Status scan(String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        LOGGER.error("Operation [SCAN] is not supported");
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public void cleanup() {
        LOGGER.info("Cleaning up YCSB client");

        if (YcsbProperties.phase == YcsbRunner.Phase.RUN) {
            client.endBenchmark();
        }

        client.close();
    }

    private static String generateKey(String table, String key) {
        return table.concat(NAMESPACE_SEPARATOR).concat(key);
    }

}
