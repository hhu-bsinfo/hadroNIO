package de.hhu.bsinfo.hadronio.jucx;

import java.io.IOException;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointErrorHandler;
import org.openucx.jucx.ucs.UcsConstants.STATUS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JucxErrorHandler implements UcpEndpointErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JucxErrorHandler.class);

    private final JucxSocketChannel channel;

    public JucxErrorHandler(JucxSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onError(UcpEndpoint endpoint, int status, String message) throws Exception {
        if (status == STATUS.UCS_ERR_CONNECTION_RESET || status == STATUS.UCS_ERR_ENDPOINT_TIMEOUT) {
            LOGGER.error("A fatal UCX error occurred! Status: [{}], Error: [{}] -> Closing channel", status, message);
            channel.close();
        } else {
            throw new IOException("A UCX error occurred! Status: [" + status + "], Error: [" + message + "]!");
        }
    }

}
