package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;

public class UcxSelector extends AbstractSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcxSelector.class);

    private final Set<SelectionKey> keys = new HashSet<>();
    private final UngrowableSelectionKeySet selectedKeys = new UngrowableSelectionKeySet();

    private boolean selectorClosed = false;
    private boolean wakeup = false;

    protected UcxSelector(SelectorProvider provider) {
        super(provider);
    }

    @Override
    protected void implCloseSelector() throws IOException {
        LOGGER.info("Closing selector");
        selectorClosed = true;

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    synchronized (cancelledKeys()) {

                        for (SelectionKey key : keys) {
                            key.cancel();
                        }

                        selectedKeys.clear();
                        keys.clear();
                    }
                }
            }
        }
    }

    @Override
    protected SelectionKey register(AbstractSelectableChannel channel, int interestOps, Object attachment) {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        UcxSelectionKey key = new UcxSelectionKey(channel, this);

        key.interestOps(interestOps);
        key.attach(attachment);

        LOGGER.info("Registering new channel with selection key [{}]", key);
        keys.add(key);
        return key;
    }

    @Override
    public Set<SelectionKey> keys() {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        return Collections.unmodifiableSet(keys);
    }

    @Override
    public Set<SelectionKey> selectedKeys() {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        return selectedKeys;
    }

    @Override
    public int selectNow() throws IOException {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    int ret = 0;

                    synchronized (cancelledKeys()) {
                        if (!cancelledKeys().isEmpty()) {
                            keys.removeAll(cancelledKeys());
                            selectedKeys.removeAll(cancelledKeys());
                        }
                    }

                    for (SelectionKey key : Collections.unmodifiableSet(keys)) {
                        ((UcxSelectableChannel) key.channel()).select();

                        if (selectKey((UcxSelectionKey) key)) {
                            ret++;
                        }
                    }

                    synchronized (cancelledKeys()) {
                        if (!cancelledKeys().isEmpty()) {
                            keys.removeAll(cancelledKeys());
                            selectedKeys.removeAll(cancelledKeys());
                        }
                    }

                    return ret;
                }
            }
        }
    }

    @Override
    public int select(long timeout) throws IOException {
        long endTime = System.nanoTime() + timeout;

        int ret = 0;

        while (selectedKeys.isEmpty() && (System.nanoTime() < endTime || timeout == 0) && !Thread.interrupted() && !wakeup) {
            ret += selectNow();
        }

        wakeup = false;
        return ret;
    }

    @Override
    public int select() throws IOException {
        return select(0);
    }

    private boolean selectKey(UcxSelectionKey key) {
        int oldReadyOps = key.readyOps();
        int readyOps = ((UcxSelectableChannel) key.channel()).readyOps();

        if ((readyOps & key.interestOps()) != 0) {
            if (selectedKeys.contains(key)) {
                key.readyOpsOr(readyOps);
            } else {
                selectedKeys.addKey(key);
                key.readyOps(readyOps);
            }
        }

        return oldReadyOps != key.readyOps();
    }

    @Override
    public Selector wakeup() {
        wakeup = true;
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
