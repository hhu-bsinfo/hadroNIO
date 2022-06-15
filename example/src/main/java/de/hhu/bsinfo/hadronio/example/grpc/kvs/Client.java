package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyRequest;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyValueRequest;
import de.hhu.bsinfo.hadronio.example.grpc.kv.KeyValueStoreGrpc;
import de.hhu.bsinfo.hadronio.example.grpc.kv.ValueResponse;
import de.hhu.bsinfo.hadronio.util.ObjectConverter;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Optional;

public class Client {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final ObjectConverter converter = new ObjectConverter();
    private KeyValueStoreGrpc.KeyValueStoreBlockingStub blockingStub;

    public void connect(final InetSocketAddress remoteAddress) {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final EventLoopGroup workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final Channel channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                .eventLoopGroup(workerGroup)
                .channelType(NioSocketChannel.class)
                .usePlaintext()
                .build();

        blockingStub = KeyValueStoreGrpc.newBlockingStub(channel);
    }

    public Status insert(final Object key, final Object value) {
        final ByteString keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final ByteString valueBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(value));
        final KeyValueRequest request = KeyValueRequest.newBuilder().setKey(keyBytes).setValue(valueBytes).build();
        return Status.fromCodeValue(blockingStub.insert(request).getStatus());
    }

    public Status update(final Object key, final Object value) {
        final ByteString keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final ByteString valueBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(value));
        final KeyValueRequest request = KeyValueRequest.newBuilder().setKey(keyBytes).setValue(valueBytes).build();
        return Status.fromCodeValue(blockingStub.update(request).getStatus());
    }

    public <T> T get(final Object key) {
        final ByteString keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final KeyRequest request = KeyRequest.newBuilder().setKey(keyBytes).build();
        final ValueResponse response = blockingStub.get(request);

        if (Status.fromCodeValue(response.getStatus()).isOk()) {
            return converter.deserialize(response.getValue().asReadOnlyByteBuffer());
        }

        return null;
    }

    public Status delete(final Object key) {
        final ByteString keyBytes = UnsafeByteOperations.unsafeWrap(converter.serialize(key));
        final KeyRequest request = KeyRequest.newBuilder().setKey(keyBytes).build();
        return Status.fromCodeValue(blockingStub.delete(request).getStatus());
    }
}
