package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyRequest;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyValueRequest;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyValueStoreGrpc;
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

public class Client implements Closeable {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final ObjectConverter converter = new ObjectConverter();
    private KeyValueStoreGrpc.KeyValueStoreBlockingStub blockingStub;

    public void connect(final InetSocketAddress remoteAddress) {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final var channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                .eventLoopGroup(workerGroup)
                .channelType(NioSocketChannel.class)
                .usePlaintext()
                .build();

        blockingStub = KeyValueStoreGrpc.newBlockingStub(channel);
    }

    public Status insert(final Object key, final Object value) {
        final var keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final var valueBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(value));
        final var request = KeyValueRequest.newBuilder().setKey(keyBytes).setValue(valueBytes).build();
        return Status.fromCodeValue(blockingStub.insert(request).getStatus());
    }

    public Status update(final Object key, final Object value) {
        final var keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final var valueBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(value));
        final var request = KeyValueRequest.newBuilder().setKey(keyBytes).setValue(valueBytes).build();
        return Status.fromCodeValue(blockingStub.update(request).getStatus());
    }

    public <T> T get(final Object key) {
        final var keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final var request = KeyRequest.newBuilder().setKey(keyBytes).build();
        final var response = blockingStub.get(request);

        if (Status.fromCodeValue(response.getStatus()).isOk()) {
            return converter.deserialize(response.getValue().asReadOnlyByteBuffer());
        }

        return null;
    }

    public Status delete(final Object key) {
        final var keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final var request = KeyRequest.newBuilder().setKey(keyBytes).build();
        return Status.fromCodeValue(blockingStub.delete(request).getStatus());
    }

    @Override
    public void close() {
        try {
            ((ManagedChannel) blockingStub.getChannel()).shutdown();
        } catch (StatusRuntimeException ignored) {}
    }

    public void shutdownServer() {
        try {
            blockingStub.shutdown(Empty.newBuilder().build());
        } catch (StatusRuntimeException ignored) {}
    }
}
