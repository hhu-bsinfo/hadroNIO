package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_ACCEPTOR_THREADS", "1"));
    private static final int WORKER_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_WORKER_THREADS", "0"));
    private static final Logger LOGGER = LoggerFactory.getLogger(de.hhu.bsinfo.hadronio.example.grpc.echo.Server.class);

    private final InetSocketAddress bindAddress;
    private final int answerSize;

    private io.grpc.Server server;

    public Server(InetSocketAddress bindAddress, int answerSize) {
        this.bindAddress = bindAddress;
        this.answerSize = answerSize;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on port [{}]", bindAddress.getPort());
        final var acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        server = NettyServerBuilder.forPort(bindAddress.getPort())
                .bossEventLoopGroup(acceptorGroup)
                .workerEventLoopGroup(workerGroup)
                .channelType(NioServerSocketChannel.class)
                .addService(new BenchmarkImpl(answerSize))
                .build();

        try {
            server.start();
        } catch (IOException e) {
            LOGGER.error("Unable to start gRPC server", e);
            return;
        }

        LOGGER.info("Server is running");

        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            LOGGER.error("Server got interrupted unexpectedly", e);
        }
    }

    private final class BenchmarkImpl extends BenchmarkGrpc.BenchmarkImplBase {
        private final BenchmarkMessage answer;

        public BenchmarkImpl(int requestSize) {
            final var answerBuffer = new byte[requestSize];
            for (int i = 0; i < requestSize; i++) {
                answerBuffer[i] = (byte) i;
            }

            answer = BenchmarkMessage.newBuilder().setContent(ByteString.copyFrom(answerBuffer)).build();
        }

        @Override
        public void benchmark(final BenchmarkMessage request, final StreamObserver<BenchmarkMessage> responseObserver) {
            responseObserver.onNext(answer);
            responseObserver.onCompleted();
        }

        @Override
        public void shutdown(final Empty request, final StreamObserver<Empty> responseObserver) {
            LOGGER.info("Close");
            responseObserver.onCompleted();
            server.shutdownNow();
        }
    }
}
