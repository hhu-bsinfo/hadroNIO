package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.protobuf.ByteString;
import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.LatencyResult;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CyclicBarrier;

public class Client implements Runnable {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress remoteAddress;
    private final int warmupCount;
    private final int requestCount;
    private final int requestSize;
    private final int answerSize;
    private final int connections;
    private final CyclicBarrier benchmarkBarrier;

    public Client(final InetSocketAddress remoteAddress, final int requestCount, final int requestSize, final int answerSize, final int connections) {
        this.remoteAddress = remoteAddress;
        this.warmupCount = requestCount / 10 == 0 ? 1 : requestCount / 10;
        this.requestCount = requestCount;
        this.requestSize = requestSize;
        this.answerSize = answerSize;
        this.connections = connections;
        benchmarkBarrier = new CyclicBarrier(connections);
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final LatencyCombiner combiner = new LatencyCombiner();
        final Runnable[] runnables = new Runnable[connections];
        final Thread[] threads = new Thread[connections];
        final EventLoopGroup workerGroup = new NioEventLoopGroup(connections);
        for (int i = 0; i < connections; i++) {
            final Channel channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                    .eventLoopGroup(workerGroup)
                    .channelType(NioSocketChannel.class)
                    .usePlaintext()
                    .build();
            runnables[i] = new BlockingRunnable(channel, benchmarkBarrier, combiner, requestCount, requestSize, answerSize);
            threads[i] = new Thread(runnables[i]);
        }

        for (int i = 0; i < connections; i++) {
            threads[i].start();
        }

        for (int i = 0; i < connections; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to join thread [{}]", threads[i].toString(), e);
            }
        }

        LOGGER.info("{}", combiner.getCombinedResult());
    }
}
