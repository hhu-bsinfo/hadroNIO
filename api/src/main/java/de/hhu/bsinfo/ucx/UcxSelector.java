package de.hhu.bsinfo.ucx;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;

public class UcxSelector extends AbstractSelector {

    private final Set<UcxSelectionKey> keys = new HashSet<>();
    private final UngrowableSelectionKeySet selectedKeys = new UngrowableSelectionKeySet();

    protected UcxSelector(SelectorProvider provider) {
        super(provider);
    }

    @Override
    protected void implCloseSelector() throws IOException {

    }

    @Override
    protected SelectionKey register(AbstractSelectableChannel channel, int interestOps, Object attachment) {
        UcxSelectionKey key = new UcxSelectionKey(channel, this);

        key.interestOps(interestOps);
        key.attach(attachment);

        keys.add(key);

        return key;
    }

    @Override
    public Set<SelectionKey> keys() {
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public Set<SelectionKey> selectedKeys() {
        return selectedKeys;
    }

    @Override
    public int selectNow() throws IOException {
        int ret = 0;

        for (UcxSelectionKey key : keys) {
            if (!key.isValid()) {
                keys.remove(key);
                selectedKeys.remove(key);
            }

            int oldReadyOps = key.readyOps();
            int readyOps = ((UcxSelectableChannel) key.channel()).readyOps();

            if ((readyOps & key.interestOps()) != 0) {
                if (selectedKeys.contains(key)) {
                    key.readyOpsOr(readyOps);
                } else {
                    selectedKeys.addKey(key);
                    key.readyOps(readyOps);
                }

                if (oldReadyOps != key.readyOps()) {
                    ret++;
                }
            }
        }

        return ret;
    }

    @Override
    public int select(long timeout) throws IOException {
        return 0;
    }

    @Override
    public int select() throws IOException {
        return select(0);
    }

    @Override
    public Selector wakeup() {
        return this;
    }

    private static final class UngrowableSelectionKeySet extends HashSet<SelectionKey> {

        public UngrowableSelectionKeySet() {
            super();
        }

        @Override
        public boolean add(SelectionKey key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends SelectionKey> keys) {
            throw new UnsupportedOperationException();
        }

        private void addKey(SelectionKey key) {
            super.add(key);
        }
    }
}
