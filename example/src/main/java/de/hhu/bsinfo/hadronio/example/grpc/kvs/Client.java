package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import de.hhu.bsinfo.hadronio.util.ObjectConverter;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Client implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final ObjectConverter converter = new ObjectConverter();
    private MessageDigest messageDigest;
    private KeyValueStoreGrpc.KeyValueStoreBlockingStub[] blockingStubs;
    private ClientIdMessage[] ids;
    private long[] buckets;

    public void connect(final InetSocketAddress[] remoteAddresses) {
        final var workerGroup = new NioEventLoopGroup(remoteAddresses.length);
        blockingStubs = new KeyValueStoreGrpc.KeyValueStoreBlockingStub[remoteAddresses.length];
        ids = new ClientIdMessage[remoteAddresses.length];
        buckets = new long[remoteAddresses.length];

        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < remoteAddresses.length; i++) {
            final var remoteAddress = remoteAddresses[i];
            LOGGER.info("Connecting to server [{}]", remoteAddress);

            final var channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                    .eventLoopGroup(workerGroup)
                    .channelType(NioSocketChannel.class)
                    .usePlaintext()
                    .build();

            blockingStubs[i] = KeyValueStoreGrpc.newBlockingStub(channel);
            buckets[i] = i == remoteAddresses.length - 1 ? Long.MAX_VALUE : Long.MIN_VALUE + (Long.MAX_VALUE / buckets.length) * (i + 1) * 2;
        }
    }

    public void startBenchmark() {
        for (int i = 0; i < blockingStubs.length; i++) {
            ids[i] = blockingStubs[i].connect(Empty.newBuilder().build());
            while (!blockingStubs[i].startBenchmark(ids[i]).getStart()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public Status insert(final Object key, final Object value) {
        final var keyBytes = converter.serialize(key);
        final var valueBytes = converter.serialize(value);
        final var request = KeyValueRequest.newBuilder()
                .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
                .setValue(UnsafeByteOperations.unsafeWrap(valueBytes))
                .build();

        return Status.fromCodeValue(getServer(keyBytes.array()).insert(request).getStatus());
    }

    public Status update(final Object key, final Object value) {
        final var keyBytes = converter.serialize(key);
        final var valueBytes = converter.serialize(value);
        final var request = KeyValueRequest.newBuilder()
                .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
                .setValue(UnsafeByteOperations.unsafeWrap(valueBytes))
                .build();

        return Status.fromCodeValue(getServer(keyBytes.array()).update(request).getStatus());
    }

    public <T> T get(final Object key) {
        final var keyBytes = converter.serialize(key);
        final var request = KeyRequest.newBuilder()
                .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
                .build();

        final var response = getServer(keyBytes.array()).get(request);
        if (Status.fromCodeValue(response.getStatus()).isOk()) {
            return converter.deserialize(response.getValue().asReadOnlyByteBuffer());
        }

        return null;
    }

    public Status delete(final Object key) {
        final var keyBytes = converter.serialize(key);
        final var request = KeyRequest.newBuilder()
                .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
                .build();

        return Status.fromCodeValue(getServer(keyBytes.array()).delete(request).getStatus());
    }

    public void endBenchmark() {
        for (int i = 0; i < blockingStubs.length; i++) {
            blockingStubs[i].endBenchmark(ids[i]);
        }
    }

    @Override
    public void close() {
        for (final var blockingStub : blockingStubs) {
            try {
                ((ManagedChannel) blockingStub.getChannel()).shutdown();
            } catch (StatusRuntimeException ignored) {}
        }
    }

    private KeyValueStoreGrpc.KeyValueStoreBlockingStub getServer(final byte[] key) {
        final var fullHash = messageDigest.digest(key);
        final long hash = (long) (fullHash[0] & 0xff) | (long) (fullHash[1] & 0xff) << 8 |
                (long) (fullHash[2] & 0xff) << 16 | (long) (fullHash[3] & 0xff) << 24 |
                (long) (fullHash[4] & 0xff) << 32 | (long) (fullHash[5] & 0xff) << 40 |
                (long) (fullHash[6] & 0xff) << 48 | (long) (fullHash[7] & 0xff) << 56;

        int index;
        for (index = 0; hash > buckets[index]; index++) {}
        return blockingStubs[index];
    }
}
