package de.hhu.bsinfo.hadronio;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxSocketChannel extends UcxSelectableChannel {

    boolean isConnected();

    void connect(InetSocketAddress remoteAddress, UcxConnectionCallback callback) throws IOException;

    boolean sendTaggedMessage(long address, long size, long tag, boolean useCallback, boolean blocking) throws IOException;

    boolean receiveTaggedMessage(long address, long size, long tag, long tagMask, boolean useCallback, boolean blocking) throws IOException;

    void setSendCallback(UcxSendCallback sendCallback);

    void setReceiveCallback(UcxReceiveCallback receiveCallback);

    UcxWorker getWorker();
}
