package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.*;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class JucxProvider implements UcxProvider {

    private final SelectorProvider provider;
    private final UcpContext context;

    public JucxProvider(final SelectorProvider provider) {
        this.provider = provider;
        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());
    }

    @Override
    public UcxServerSocketChannel createServerSocketChannel() {
        return new JucxServerSocketChannel(provider, context);
    }

    @Override
    public UcxSocketChannel createSocketChannel() {
        return new JucxSocketChannel(context);
    }
}
