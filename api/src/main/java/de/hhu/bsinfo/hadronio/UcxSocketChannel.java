package de.hhu.bsinfo.hadronio;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public interface UcxSocketChannel extends Closeable {

    boolean isConnected();

    boolean isConnectionPending();

    void connect(InetSocketAddress remoteAddress, UcxCallback callback) throws IOException;

    boolean finishConnect() throws IOException;

    void sendTaggedMessage(long address, long size, long tag);

    void receiveTaggedMessage(long address, long size, long tag, long tagMask);

    void configureBlocking(boolean blocking);

    void pollWorkerBlocking() throws IOException;

    void pollWorkerNonBlocking() throws IOException;

    void interruptPolling();

    void setSendCallback(UcxCallback sendCallback);

    void setReceiveCallback(UcxCallback receiveCallback);
}
