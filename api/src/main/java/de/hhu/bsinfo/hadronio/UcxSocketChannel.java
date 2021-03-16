package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxSocketChannel extends UcxSelectableChannel, Closeable {

    boolean isConnected();

    void connect(InetSocketAddress remoteAddress, UcxCallback callback) throws IOException;

    void sendTaggedMessage(long address, long size, long tag);

    void receiveTaggedMessage(long address, long size, long tag, long tagMask);

    void setSendCallback(UcxCallback sendCallback);

    void setReceiveCallback(UcxCallback receiveCallback);
}
