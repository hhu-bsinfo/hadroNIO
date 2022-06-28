package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(de.hhu.bsinfo.hadronio.example.grpc.echo.Server.class);

    private final InetSocketAddress bindAddress;
    private final int answerSize;

    public Server(InetSocketAddress bindAddress, int answerSize) {
        this.bindAddress = bindAddress;
        this.answerSize = answerSize;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on port [{}]", bindAddress.getPort());
        final EventLoopGroup acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        final io.grpc.Server server = NettyServerBuilder.forPort(bindAddress.getPort())
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

    private static final class BenchmarkImpl extends BenchmarkGrpc.BenchmarkImplBase {
        private final BenchmarkMessage answer;

        public BenchmarkImpl(int requestSize) {
            final byte[] answerBuffer = new byte[requestSize];
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
    }
}
