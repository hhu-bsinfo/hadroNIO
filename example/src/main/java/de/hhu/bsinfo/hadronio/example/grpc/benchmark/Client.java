package de.hhu.bsinfo.hadronio.example.grpc.benchmark;

import com.google.protobuf.Empty;
import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.ThroughputCombiner;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CyclicBarrier;

public class Client implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final InetSocketAddress remoteAddress;
    private final int requestCount;
    private final int requestSize;
    private final int answerSize;
    private final int connections;
    private final int aggregationThreshold;
    private final boolean blocking;
    private final CyclicBarrier benchmarkBarrier;

    private final String resultFileName;
    private final String benchmarkName;
    private final int benchmarkIteration;

    public Client(final InetSocketAddress remoteAddress, final int requestCount, final int requestSize, final int answerSize, final int connections,
                  final int aggregationThreshold, final boolean blocking, final String resultFileName, final String benchmarkName, final int benchmarkIteration) {
        this.remoteAddress = remoteAddress;
        this.requestCount = requestCount;
        this.requestSize = requestSize;
        this.answerSize = answerSize;
        this.connections = connections;
        this.aggregationThreshold = aggregationThreshold;
        this.blocking = blocking;
        this.resultFileName = resultFileName;
        this.benchmarkName = benchmarkName;
        this.benchmarkIteration = benchmarkIteration;
        benchmarkBarrier = new CyclicBarrier(connections);
    }

    @Override
    public void run() {
        LOGGER.info("Connecting to server [{}]", remoteAddress);
        final var combiner = blocking ? new LatencyCombiner() : new ThroughputCombiner();
        final var runnables = new Runnable[connections];
        final var threads = new Thread[connections];
        final var workerGroup = new NioEventLoopGroup(connections);

        for (int i = 0; i < connections; i++) {
            final var channel = NettyChannelBuilder.forAddress(remoteAddress.getHostString(), remoteAddress.getPort())
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

        final var result = combiner.getCombinedResult();
        LOGGER.info("{}", result);

        if (!resultFileName.isEmpty()) {
            try {
                result.writeToFile(resultFileName, benchmarkName, benchmarkIteration, connections);
            } catch (IOException e) {
                LOGGER.error("Unable to write result to file '{}'", resultFileName, e);
            }
        }
    }
}
