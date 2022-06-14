package de.hhu.bsinfo.hadronio.example.grpc.echo;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
        final EventLoopGroup workerGroup = new NioEventLoopGroup(WORKER_THREADS);
        final Channel channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                .eventLoopGroup(workerGroup)
                .channelType(NioSocketChannel.class)
                .usePlaintext()
                .build();

        final EchoGrpc.EchoBlockingStub blockingStub = EchoGrpc.newBlockingStub(channel);
        final Scanner scanner = new Scanner(System.in);

        while (scanner.hasNextLine()) {
            try {
                final String line = scanner.nextLine();
                final EchoMessage request = EchoMessage.newBuilder().setMessage(line).build();
                final EchoMessage response = blockingStub.echo(request);
                LOGGER.info("Received echo response [{}]", response.getMessage());
            } catch (StatusRuntimeException e) {
                LOGGER.error("Failed to execute RPC", e);
            }
        }
    }
}
