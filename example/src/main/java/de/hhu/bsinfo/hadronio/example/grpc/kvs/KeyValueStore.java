package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

class KeyValueStore extends KeyValueStoreGrpc.KeyValueStoreImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueStore.class);

    private final ConcurrentMap<ByteBuffer, ByteBuffer> store = new ConcurrentHashMap<>();
    private final AtomicInteger counter = new AtomicInteger();

    private Server server;
    private int connections;

    void setServer(final Server server) {
        this.server = server;
    }

    public void setConnections(int connections) {
        this.connections = connections;
    }

    @Override
    public void insert(final KeyValueRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final var key = request.getKey().asReadOnlyByteBuffer();
        final var value = request.getValue().asReadOnlyByteBuffer();

        final var oldValue = store.putIfAbsent(key, value);
        if (oldValue == null) {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.OK.value()).build());
        } else {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.ALREADY_EXISTS.value()).build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void update(final KeyValueRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final var key = request.getKey().asReadOnlyByteBuffer();
        final var value = request.getValue().asReadOnlyByteBuffer();

        ByteBuffer oldValue;
        do {
            oldValue = store.get(key);
            if (oldValue == null) {
                responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.NOT_FOUND.value()).build());
                responseObserver.onCompleted();
                return;
            }
        } while (!store.replace(key, oldValue, value));

        responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.OK.value()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(final KeyRequest request, final StreamObserver<ValueResponse> responseObserver) {
        final var key = request.getKey().asReadOnlyByteBuffer();

        final var value = store.get(key);
        if (value == null) {
            responseObserver.onNext(ValueResponse.newBuilder().setStatus(Status.Code.NOT_FOUND.value()).build());
        } else {
            responseObserver.onNext(ValueResponse.newBuilder().setStatus(Status.Code.OK.value()).setValue(UnsafeByteOperations.unsafeWrap(value)).build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void delete(final KeyRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final var key = request.getKey().asReadOnlyByteBuffer();

        final var oldValue = store.remove(key);
        if (oldValue == null) {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.NOT_FOUND.value()).build());
        } else {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.OK.value()).build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void connect(final Empty request, final StreamObserver<ClientIdMessage> responseObserver) {
        final int id = counter.incrementAndGet();
        LOGGER.info("Client [#{}] connected", id);

        responseObserver.onNext(ClientIdMessage.newBuilder().setId(id).build());
        responseObserver.onCompleted();
    }

    @Override
    public void startBenchmark(final ClientIdMessage request, final StreamObserver<StartBenchmarkMessage> responseObserver) {
        final boolean start = counter.get() >= connections;
        responseObserver.onNext(StartBenchmarkMessage.newBuilder().setStart(start).build());
        responseObserver.onCompleted();
    }

    @Override
    public void endBenchmark(final ClientIdMessage request, final StreamObserver<Empty> responseObserver) {
        LOGGER.info("Client [#{}] disconnected", request.getId());
        if (counter.decrementAndGet() <= 0) {
            server.shutdownNow();
        }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}