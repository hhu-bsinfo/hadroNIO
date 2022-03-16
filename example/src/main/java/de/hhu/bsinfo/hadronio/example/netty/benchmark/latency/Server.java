package de.hhu.bsinfo.hadronio.example.netty.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.LatencyCombiner;
import de.hhu.bsinfo.hadronio.util.LatencyResult;
import de.hhu.bsinfo.hadronio.util.NettyUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class Server implements Runnable {

    private static final int ACCEPTOR_THREADS = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final InetSocketAddress bindAddress;
    private final int messageSize;
    private final int messageCount;
    private final int connections;
    private final boolean pinThreads;

    private final String resultFileName;
    private final String benchmarkName;
    private final int benchmarkIteration;

    private int connectedChannels;
    private final Channel[] channels;

    private final Object connectionLock = new Object();
    private final Object connectionBarrier = new Object();
    private final AtomicInteger warmupCounter = new AtomicInteger();
    private final AtomicInteger benchmarkCounter = new AtomicInteger();

    public Server(final InetSocketAddress bindAddress, final int messageSize, final int messageCount, final int connections,
                  final boolean pinThreads, final String resultFileName, final String benchmarkName, final int benchmarkIteration) {
        this.bindAddress = bindAddress;
        this.messageSize = messageSize;
        this.messageCount = messageCount;
        this.connections = connections;
        this.pinThreads = pinThreads;
        this.resultFileName = resultFileName;
        this.benchmarkName = benchmarkName;
        this.benchmarkIteration = benchmarkIteration;
        channels = new Channel[connections];
    }

    @Override
    public void run() {
        LOGGER.info("Starting server on [{}]", bindAddress);
        final EventLoopGroup acceptorGroup = new NioEventLoopGroup(ACCEPTOR_THREADS);
        final EventLoopGroup workerGroup = NettyUtil.createWorkerGroup(connections, pinThreads);
        final ServerBootstrap bootstrap = new ServerBootstrap();
        final LatencyCombiner combiner = new LatencyCombiner();

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
            new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    channel.closeFuture().addListener(future -> LOGGER.info("Closed channel connected to [{}]", channel.remoteAddress()));
                    channel.pipeline().addLast(new ServerWarmupHandler(messageSize, messageCount, messageCount / 10, connections, warmupCounter, benchmarkCounter, combiner));

                    synchronized (connectionLock) {
                        channels[connectedChannels] = channel;

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

            synchronized (warmupCounter) {
                for (int i = 0; i < connections; i++) {
                    final ChannelHandlerContext context = channels[i].pipeline().context(ServerWarmupHandler.class);
                    channels[i].pipeline().get(ServerWarmupHandler.class).start(context);
                }

                warmupCounter.wait();
            }

            synchronized (benchmarkCounter) {
                for (int i = 0; i < connections; i++) {
                    final ChannelHandlerContext context = channels[i].pipeline().context(ServerHandler.class);
                    channels[i].pipeline().get(ServerHandler.class).start(context);
                }

                benchmarkCounter.wait();
            }

            for (int i = 0; i < connections; i++) {
                channels[i].closeFuture().sync();
            }

            final LatencyResult result = combiner.getCombinedResult();
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
