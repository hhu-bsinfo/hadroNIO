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

class HadronioSelector extends AbstractSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadronioSelector.class);

    private final UcxWorker worker;
    private final Set<SelectionKey> keys = new HashSet<>();
    private final UngrowableSelectionKeySet selectedKeys = new UngrowableSelectionKeySet();
    private final Object wakeupLock = new Object();

    private boolean selectorClosed = false;

    HadronioSelector(final SelectorProvider selectorProvider, final UcxWorker worker) {
        super(selectorProvider);
        this.worker = worker;
    }

    @Override
    protected void implCloseSelector() throws IOException {
        LOGGER.info("Closing selector");
        selectorClosed = true;

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    synchronized (cancelledKeys()) {
                        wakeup();

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
    protected SelectionKey register(final AbstractSelectableChannel channel, final int interestOps, final Object attachment) {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        final HadronioSelectionKey key = new HadronioSelectionKey(channel, this);

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
        return select(false, 0);
    }

    @Override
    public int select(final long timeout) throws IOException {
        return select(true, timeout);
    }

    @Override
    public int select() throws IOException {
        return select(true, 0);
    }

    @Override
    public Selector wakeup() {
        synchronized (wakeupLock) {
            worker.interrupt();
        }

        return this;
    }

    private int select(final boolean blocking, final long timeout) throws IOException {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    pollWorker(false, timeout);

                    int ret = 0;
                    do {
                        synchronized (cancelledKeys()) {
                            if (!cancelledKeys().isEmpty()) {
                                keys.removeAll(cancelledKeys());
                                selectedKeys.removeAll(cancelledKeys());
                                cancelledKeys().clear();
                            }
                        }

                        for (SelectionKey key : Collections.unmodifiableSet(keys)) {
                            ((HadronioSelectableChannel) key.channel()).select();

                            if (selectKey((HadronioSelectionKey) key)) {
                                ret++;
                            }
                        }

                        synchronized (cancelledKeys()) {
                            if (!cancelledKeys().isEmpty()) {
                                keys.removeAll(cancelledKeys());
                                selectedKeys.removeAll(cancelledKeys());
                                cancelledKeys().clear();
                            }
                        }

                        if (blocking && keys.size() > 0 && selectedKeys.size() == 0) {
                            boolean eventsPolled = pollWorker(true, timeout);
                            if (!eventsPolled) {
                                // Worker has been interrupted by wakeup()
                                break;
                            }
                        }
                    } while (blocking && keys.size() > 0 && selectedKeys.size() == 0);

                    return ret;
                }
            }
        }
    }

    private boolean pollWorker(final boolean blocking, final long timeout) {
        // TODO: Implement timeout
        try {
            return worker.poll(blocking);
        } catch (IOException e) {
            LOGGER.error("Failed to poll worker (Message: [{}])", e.getMessage());
            LOGGER.debug("Stack trace:", e);
        }

        return false;
    }

    private boolean selectKey(final HadronioSelectionKey key) {
        final int oldReadyOps = key.readyOps();
        final int readyOps = ((HadronioSelectableChannel) key.channel()).readyOps() & key.interestOps();

        if (readyOps != 0) {
            if (selectedKeys.contains(key)) {
                key.readyOpsOr(readyOps);
            } else {
                selectedKeys.addKey(key);
                key.readyOps(readyOps);
            }
        }

        return oldReadyOps != key.readyOps();
    }

    private static final class UngrowableSelectionKeySet extends HashSet<SelectionKey> {

        public UngrowableSelectionKeySet() {
            super();
        }

        @Override
        public boolean add(final SelectionKey key) {
            throw new UnsupportedOperationException("Trying to add a key to an ungrowable set!");
        }

        @Override
        public boolean addAll(final Collection<? extends SelectionKey> keys) {
            throw new UnsupportedOperationException("Trying to add a key to an ungrowable set!");
        }

        private void addKey(final SelectionKey key) {
            super.add(key);
        }
    }
}
