package de.hhu.bsinfo.hadronio.example.netty.benchmark.throughput;

import de.hhu.bsinfo.hadronio.util.ThroughputCombiner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CyclicBarrier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;
    private final int messageSize;
    private final int messageCount;
    private final int aggregationThreshold;
    private final int connections;

    private final String resultFileName;
    private final String benchmarkName;
    private final int benchmarkIteration;

    private int connectedChannels;
    private final SendRunnable[] runnables;
    private final Thread[] threads;

    private final Object connectionLock = new Object();
    private final Object connectionBarrier = new Object();
    private final CyclicBarrier benchmarkBarrier;

    public Server(final InetSocketAddress bindAddress, final int messageSize, final int messageCount, final int aggregationThreshold,
                  final int connections, final String resultFileName, final String benchmarkName, final int benchmarkIteration) {
        this.bindAddress = bindAddress;
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.aggregationThreshold = aggregationThreshold;
        this.connections = connections;
        this.resultFileName = resultFileName;
        this.benchmarkName = benchmarkName;
        this.benchmarkIteration = benchmarkIteration;
        runnables = new SendRunnable[connections];
        threads = new Thread[connections];
        benchmarkBarrier = new CyclicBarrier(connections);
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on [{}]", bindAddress);
        final var acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final var workerGroup = new NioEventLoopGroup();
        final var bootstrap = new ServerBootstrap();
        final var combiner = new ThroughputCombiner();

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
            new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(@NotNull SocketChannel channel) {
                    final Object syncLock = new Object();
                    channel.closeFuture().addListener(future -> LOGGER.info("Closed channel connected to [{}]", channel.remoteAddress()));
                    channel.pipeline().addLast(new ServerHandler(syncLock));

                    synchronized (connectionLock) {
                        runnables[connectedChannels] = new SendRunnable(messageSize, messageCount, aggregationThreshold, syncLock, benchmarkBarrier, channel, combiner);
                        threads[connectedChannels] = new Thread(runnables[connectedChannels]);

                        if (++connectedChannels == connections) {
                            synchronized (connectionBarrier) {
                                connectionBarrier.notify();
                            }
                        }
                    }
                }
            });

        final Channel serverChannel = bootstrap.bind(bindAddress).addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.info("Server is running");
            } else {
                LOGGER.error("Unable to start server", future.cause());
            }
        }).channel();
        serverChannel.closeFuture().addListener(future -> LOGGER.info("Server channel closed"));

        try {
            synchronized (connectionBarrier) {
                connectionBarrier.wait();
            }

            serverChannel.close();

            for (int i = 0; i < connections; i++) {
                threads[i].start();
            }

            for (int i = 0; i < connections; i++) {
                threads[i].join();
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
        } catch (InterruptedException e) {
            LOGGER.error("A sync error occurred", e);
        } finally {
            acceptorGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
