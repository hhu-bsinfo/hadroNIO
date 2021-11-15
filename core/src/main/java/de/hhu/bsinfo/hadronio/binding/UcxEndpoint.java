package de.hhu.bsinfo.hadronio.binding;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public interface UcxEndpoint extends UcxProgressable {

    void connect(InetSocketAddress remoteAddress) throws IOException;

    boolean sendTaggedMessage(long address, long size, long tag, boolean useCallback, boolean blocking) throws IOException;

    boolean receiveTaggedMessage(long address, long size, long tag, long tagMask, boolean useCallback, boolean blocking) throws IOException;

    void sendStream(long address, long size, UcxCallback callback);

    void receiveStream(long address, long size, UcxCallback callback);

    void setSendCallback(UcxCallback sendCallback);

    void setReceiveCallback(UcxReceiveCallback receiveCallback);

    boolean isClosed();
}
