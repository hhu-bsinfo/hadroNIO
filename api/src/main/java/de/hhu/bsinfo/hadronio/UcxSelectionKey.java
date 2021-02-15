package de.hhu.bsinfo.hadronio;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectionKey;

public class UcxSelectionKey extends AbstractSelectionKey {

    private final SelectableChannel channel;
    private final Selector selector;

    private int interestOps = 0;
    private int readyOps = 0;
    private boolean open = true;

    public UcxSelectionKey(SelectableChannel channel, Selector selector) {
        this.channel = channel;
        this.selector = selector;
    }

    @Override
    public SelectableChannel channel() {
        return channel;
    }

    @Override
    public Selector selector() {
        return selector;
    }

    @Override
    public int interestOps() {
        return interestOps;
    }

    @Override
    public SelectionKey interestOps(int interestOps) {
        synchronized (this) {
            this.interestOps = interestOps;
        }

        return this;
    }

    @Override
    public int readyOps() {
        return readyOps;
    }

    @Override
    public String toString() {
        return "UcxSelectionKey(interestOps=" + interestOps + ", readyOps=" + readyOps + ", isValid=" + isValid() + ")";
    }

    protected void readyOps(int readyOps) {
        synchronized(this) {
            this.readyOps = readyOps;
        }
    }

    protected void readyOpsOr(int readyOps) {
        synchronized(this) {
            this.readyOps(this.readyOps | readyOps);
        }
    }
}
