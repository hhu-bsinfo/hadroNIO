package de.hhu.bsinfo.hadronio.example.grpc.echo;

import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class Client implements Runnable {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress remoteAddress;

    public Client(final InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final var workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final var channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                .eventLoopGroup(workerGroup)
                .channelType(NioSocketChannel.class)
                .usePlaintext()
                .build();

        final var blockingStub = EchoGrpc.newBlockingStub(channel);
        final var scanner = new Scanner(System.in);

        while (scanner.hasNextLine()) {
            try {
                final var line = scanner.nextLine();
                final var request = EchoMessage.newBuilder().setMessage(line).build();
                final var response = blockingStub.echo(request);
                LOGGER.info("Received echo response [{}]", response.getMessage());
            } catch (StatusRuntimeException e) {
                LOGGER.error("Failed to execute RPC", e);
            }
        }
    }
}
