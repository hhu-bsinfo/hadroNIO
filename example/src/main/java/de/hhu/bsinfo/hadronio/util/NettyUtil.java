package de.hhu.bsinfo.hadronio.util;

import de.hhu.bsinfo.hadronio.example.netty.benchmark.latency.Client;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

public class NettyUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyUtil.class);

    public static EventLoopGroup createWorkerGroup(final int connections, final boolean pinThreads) {
        EventLoopGroup workerGroup;

        if (pinThreads) {
            LOGGER.info("Thread pinning is activated for netty worker threads");
            final ThreadFactory threadFactory = new AffinityThreadFactory("workerFactory", AffinityStrategies.DIFFERENT_CORE);
            workerGroup = new NioEventLoopGroup(connections, threadFactory);
        } else {
            LOGGER.info("Thread pinning is not activated");
            workerGroup = new NioEventLoopGroup(connections);
        }

        return workerGroup;
    }
}
