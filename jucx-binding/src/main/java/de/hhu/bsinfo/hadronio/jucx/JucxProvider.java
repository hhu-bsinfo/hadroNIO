package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxProvider;
import de.hhu.bsinfo.hadronio.HadronioServerSocketChannel;
import de.hhu.bsinfo.hadronio.HadronioSocketChannel;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class JucxProvider implements UcxProvider {

    private final SelectorProvider provider;
    private final UcpContext context;

    private final int sendBufferLength;
    private final int receiveBufferLength;
    private final int bufferSliceLength;

    public JucxProvider(final SelectorProvider provider, final int sendBufferLength, final int receiveBufferLength, final int bufferSliceLength) {
        this.provider = provider;
        this.sendBufferLength = sendBufferLength;
        this.receiveBufferLength = receiveBufferLength;
        this.bufferSliceLength = bufferSliceLength;

        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());
    }

    @Override
    public ServerSocketChannel createServerSocketChannel() {
        final JucxServerSocketChannel serverSocketChannel = new JucxServerSocketChannel(provider, context, sendBufferLength, receiveBufferLength, bufferSliceLength);
        return new HadronioServerSocketChannel(provider, serverSocketChannel);
    }

    @Override
    public SocketChannel createSocketChannel() {
        final JucxSocketChannel socketChannel = new JucxSocketChannel(context);
        return new HadronioSocketChannel(provider, socketChannel, sendBufferLength, receiveBufferLength, bufferSliceLength);
    }
}
