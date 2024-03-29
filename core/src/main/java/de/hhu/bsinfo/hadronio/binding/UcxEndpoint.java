package de.hhu.bsinfo.hadronio.binding;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxEndpoint extends UcxProgressable {

    void connect(InetSocketAddress remoteAddress) throws IOException;

    boolean sendTaggedMessage(long address, long size, long tag, boolean useCallback, boolean blocking);

    boolean receiveTaggedMessage(long address, long size, long tag, long tagMask, boolean useCallback, boolean blocking);

    boolean sendStream(long address, long size, boolean useCallback, boolean blocking);

    boolean receiveStream(long address, long size, boolean useCallback, boolean blocking);

    void setSendCallback(UcxSendCallback sendCallback);

    void setReceiveCallback(UcxReceiveCallback receiveCallback);

    boolean getErrorState();

    InetSocketAddress getRemoteAddress();
}
