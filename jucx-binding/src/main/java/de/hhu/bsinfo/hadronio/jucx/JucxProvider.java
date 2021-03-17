package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.*;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

public class JucxProvider implements UcxProvider {

    private final UcpContext context;

    public JucxProvider() {
        context = new UcpContext(new UcpParams().requestWakeupFeature().requestTagFeature());
    }

    @Override
    public UcxServerSocketChannel createServerSocketChannel() {
        return new JucxServerSocketChannel(context);
    }

    @Override
    public UcxSocketChannel createSocketChannel() {
        return new JucxSocketChannel(context);
    }
}
