package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.binding.UcxRequest;
import org.openucx.jucx.ucp.UcpRequest;

public class JucxRequest implements UcxRequest {
    private final UcpRequest request;
    JucxRequest(final UcpRequest request) {
        this.request = request;
    }
    @Override
    public boolean isCompleted() {
        return request.isCompleted();
    }

    UcpRequest getRequest() {
        return request;
    }
}
