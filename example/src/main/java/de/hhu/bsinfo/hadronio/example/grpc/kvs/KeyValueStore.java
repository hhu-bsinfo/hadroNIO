package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import com.google.protobuf.UnsafeByteOperations;
import de.hhu.bsinfo.hadronio.example.grpc.kv.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class KeyValueStore extends KeyValueStoreGrpc.KeyValueStoreImplBase {

    private final ConcurrentMap<ByteBuffer, ByteBuffer> store = new ConcurrentHashMap<>();

    @Override
    public void insert(final KeyValueRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final ByteBuffer key = request.getKey().asReadOnlyByteBuffer();
        final ByteBuffer value = request.getValue().asReadOnlyByteBuffer();

        final ByteBuffer oldValue = store.putIfAbsent(key, value);
        if (oldValue == null) {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.OK.value()).build());
        } else {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.ALREADY_EXISTS.value()).build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void update(final KeyValueRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final ByteBuffer key = request.getKey().asReadOnlyByteBuffer();
        final ByteBuffer value = request.getValue().asReadOnlyByteBuffer();

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
        final ByteBuffer key = request.getKey().asReadOnlyByteBuffer();

        final ByteBuffer value = store.get(key);
        if (value == null) {
            responseObserver.onNext(ValueResponse.newBuilder().setStatus(Status.Code.NOT_FOUND.value()).build());
        } else {
            responseObserver.onNext(ValueResponse.newBuilder().setStatus(Status.Code.OK.value()).setValue(UnsafeByteOperations.unsafeWrap(value)).build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void delete(final KeyRequest request, final StreamObserver<StatusResponse> responseObserver) {
        final ByteBuffer key = request.getKey().asReadOnlyByteBuffer();

        final ByteBuffer oldValue = store.remove(key);
        if (oldValue == null) {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.NOT_FOUND.value()).build());
        } else {
            responseObserver.onNext(StatusResponse.newBuilder().setStatus(Status.Code.OK.value()).build());
        }

        responseObserver.onCompleted();
    }
}