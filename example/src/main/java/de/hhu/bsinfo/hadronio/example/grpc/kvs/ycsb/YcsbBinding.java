package de.hhu.bsinfo.hadronio.example.grpc.kvs.ycsb;

import de.hhu.bsinfo.hadronio.example.grpc.kvs.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.util.*;

public class YcsbBinding extends DB {

    private static final Logger LOGGER = LoggerFactory.getLogger(YcsbBinding.class);

    private static final String NAMESPACE_SEPARATOR = ".";

    private final de.hhu.bsinfo.hadronio.example.grpc.kvs.Client client = new Client();

    private YcsbProperties properties;
    private YcsbObject reusableObject;

    @Override
    public void init() {
        LOGGER.info("Initializing YCSB client");
        properties = new YcsbProperties(getProperties());
        reusableObject = new YcsbObject(properties.getFieldsPerKey(), properties.getFieldSize());
        client.connect(properties.getRemoteAddress());
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            reusableObject.setFieldValue(entry.getKey(), entry.getValue());
        }

        final io.grpc.Status status = client.insert(generateKey(table, key), reusableObject.getData());
        return status == io.grpc.Status.OK ? Status.OK : Status.FORBIDDEN;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            reusableObject.setFieldValue(entry.getKey(), entry.getValue());
        }

        final io.grpc.Status status = client.update(generateKey(table, key), reusableObject.getData());
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
            for (final String field : fields) {
                result.put(field, reusableObject.getFieldIterator(field));
            }
        }

        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        final io.grpc.Status status = client.delete(generateKey(table, key));
        return status == io.grpc.Status.OK ? Status.OK : Status.NOT_FOUND;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        LOGGER.error("Operation [SCAN] is not supported");
        return Status.NOT_IMPLEMENTED;
    }

    private static String generateKey(String table, String key) {
        return table.concat(NAMESPACE_SEPARATOR).concat(key);
    }

}
