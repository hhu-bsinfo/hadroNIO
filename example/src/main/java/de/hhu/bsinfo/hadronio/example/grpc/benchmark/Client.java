package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import de.hhu.bsinfo.hadronio.util.Combiner;
import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.ThroughputCombiner;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CyclicBarrier;

public class Client implements Runnable {

    private static final int WORKER_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress remoteAddress;
    private final int requestCount;
    private final int requestSize;
    private final int answerSize;
    private final int connections;
    private final int aggregationThreshold;
    private final boolean blocking;
    private final CyclicBarrier benchmarkBarrier;

    public Client(final InetSocketAddress remoteAddress, final int requestCount, final int requestSize, final int answerSize, final int connections, final int aggregationThreshold, final boolean blocking) {
        this.remoteAddress = remoteAddress;
        this.requestCount = requestCount;
        this.requestSize = requestSize;
        this.answerSize = answerSize;
        this.connections = connections;
        this.aggregationThreshold = aggregationThreshold;
        this.blocking = blocking;
        benchmarkBarrier = new CyclicBarrier(connections);
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final Combiner combiner = blocking ? new LatencyCombiner() : new ThroughputCombiner();
        final Runnable[] runnables = new Runnable[connections];
        final Thread[] threads = new Thread[connections];
        final EventLoopGroup workerGroup = new NioEventLoopGroup(connections);
        for (int i = 0; i < connections; i++) {
            final Channel channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
                    .eventLoopGroup(workerGroup)
                    .channelType(NioSocketChannel.class)
                    .usePlaintext()
                    .build();
            runnables[i] = blocking ? new BlockingRunnable(channel, benchmarkBarrier, (LatencyCombiner) combiner, requestCount, requestSize, answerSize) : new NonBlockingRunnable(channel, benchmarkBarrier, (ThroughputCombiner) combiner, requestCount, requestSize, answerSize, aggregationThreshold);
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
