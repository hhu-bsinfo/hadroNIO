package de.hhu.bsinfo.hadronio.example.grpc.echo;

import io.grpc.ServerBuilder;
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
    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;

    public Server(final InetSocketAddress bindAddress) {
        this.bindAddress = bindAddress;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on port [{}]", bindAddress.getPort());
        final EventLoopGroup acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final EventLoopGroup workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final io.grpc.Server server = NettyServerBuilder.forPort(bindAddress.getPort())
                .bossEventLoopGroup(acceptorGroup)
                .workerEventLoopGroup(workerGroup)
                .channelType(NioServerSocketChannel.class)
                .addService(new EchoImpl())
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

    private static final class EchoImpl extends EchoGrpc.EchoImplBase {

        @Override
        public void echo(final EchoMessage request, final StreamObserver<EchoMessage> responseObserver) {
            LOGGER.info("Received echo request [{}]", request.getMessage());
            final EchoMessage reply = EchoMessage.newBuilder().setMessage(request.getMessage()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
