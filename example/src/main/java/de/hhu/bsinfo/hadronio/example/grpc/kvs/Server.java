package de.hhu.bsinfo.hadronio.example.grpc.kvs;

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_ACCEPTOR_THREADS", "1"));
    private static final int WORKER_THREADS = Integer.parseInt(System.getProperty("de.hhu.bsinfo.hadronio.example.NETTY_WORKER_THREADS", "0"));
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;
    private final int connections;

    public Server(final InetSocketAddress bindAddress, int connections) {
        this.bindAddress = bindAddress;
        this.connections = connections;
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on port [{}]", bindAddress.getPort());
        final var acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final var store = new KeyValueStore();
        final var server = NettyServerBuilder.forPort(bindAddress.getPort())
                .bossEventLoopGroup(acceptorGroup)
                .workerEventLoopGroup(workerGroup)
                .executor(Executors.newFixedThreadPool(WORKER_THREADS))
                .channelType(NioServerSocketChannel.class)
                .addService(store)
                .build();

        store.setServer(server);
        store.setConnections(connections);

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
}
