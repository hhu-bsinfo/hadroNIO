package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxException;
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

    private final Set<SelectionKey> keys = new HashSet<>();
    private final FixedSelectionKeySet selectedKeys = new FixedSelectionKeySet();
    private final Object wakeupLock = new Object();

    private boolean wakeupStatus = true;
    private boolean selectorClosed = false;

    HadronioSelector(final SelectorProvider selectorProvider) {
        super(selectorProvider);
    }

    @Override
    protected void implCloseSelector() {
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
        LOGGER.info("Registering channel with selection key [{}]", key);

        synchronized (keys) {
            synchronized (wakeupLock) {
                keys.add(key);
                wakeupLock.notifyAll();

                return key;
            }
        }
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
        LOGGER.debug("Waking up worker");
        synchronized (wakeupLock) {
            wakeupStatus = true;
            wakeupLock.notifyAll();
        }

        return this;
    }

    private int select(final boolean blocking, final long timeout) throws IOException {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        LOGGER.debug("Starting select operation (blocking: [{}], timeout: [{}])", blocking, timeout);

        boolean keysAvailable = checkKeys(blocking, timeout);
        if (!keysAvailable) {
            return 0;
        }

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    pollWorker(false, 0);

                    int updatedKeys = 0;
                    do {
                        removeCancelledKeys();
                        updatedKeys += performSelectOperation();
                        removeCancelledKeys();

                        LOGGER.debug("Finished select iteration (blocking: [{}], keys: [{}], selectedKeys: [{}])", blocking, keys.size(), selectedKeys.size());

                        if (blocking && keys.size() > 0 && selectedKeys.size() == 0) {
                            pollWorker(true, timeout);

                            synchronized (wakeupLock) {
                                if (wakeupStatus) {
                                    LOGGER.debug("Selector has been interrupted by wakeup");
                                    wakeupStatus = false;
                                    break;
                                }
                            }
                        }
                    } while (blocking && keys.size() > 0 && selectedKeys.size() == 0);

                    LOGGER.debug("Finished select operation (blocking: [{}], timeout: [{}], updatedKeys: [{}])", blocking, timeout, updatedKeys);
                    return updatedKeys;
                }
            }
        }
    }

    private void pollWorker(final boolean blocking, final long timeout) {
        try {
            LOGGER.debug("Polling worker (blocking: [{}], timeout: [{}])", blocking, timeout);
            boolean eventsPolled = false;
            final long endTime = System.nanoTime() + timeout * 1000000;

            do {
                for (final SelectionKey key : keys) {
                    eventsPolled |= ((HadronioSelectableChannel) key.channel()).getWorker().progress();
                }

                if (timeout > 0 && System.nanoTime() > endTime) {
                    LOGGER.debug("Timeout of [{}] has been reached while polling worker", timeout);
                    break;
                }
            } while(blocking && !eventsPolled && !wakeupStatus);
            LOGGER.debug("Finished polling worker (eventsPolled: [{}])", eventsPolled);
        } catch (IOException | UcxException e) {
            LOGGER.error("Failed to poll worker (Message: [{}])", e.getMessage(), e);
        }
    }

    private boolean selectKey(final HadronioSelectionKey key) {
        LOGGER.debug("Selecting key: [{}]", key);
        final int channelReadyOps = ((HadronioSelectableChannel) key.channel()).readyOps();
        final int readyOps = channelReadyOps & key.interestOps();
        LOGGER.debug("Selected channel (channelReadyOps: [{}], readyOps: [{}])", channelReadyOps, readyOps);

        if (readyOps != 0) {
            if (selectedKeys.contains(key)) {
                key.readyOpsOr(readyOps);
            } else {
                selectedKeys.addKey(key);
                key.readyOps(readyOps);
            }

            return true;
        }

        return false;
    }

    private boolean checkKeys(final boolean blocking, final long timeout) {
        if (!keys.isEmpty()) {
            return true;
        }

        LOGGER.debug("No keys are registered");
        if (!blocking) {
            return false;
        }

        synchronized (wakeupLock) {
            try {
                LOGGER.debug("Waiting for new keys (timeout: [{}])", timeout);
                wakeupLock.wait(timeout);
            } catch (InterruptedException e) {
                LOGGER.warn("Thread has been interrupted while waiting for keys to be registered");
                return false;
            }

            if (wakeupStatus) {
                LOGGER.debug("Selector has been interrupted by wakeup");
                wakeupStatus = false;
                return false;
            }
        }

        if (keys.isEmpty()) {
            LOGGER.debug("There are still no keys registered after wait() has returned");
            return false;
        }

        return true;
    }

    private void removeCancelledKeys() {
        synchronized (cancelledKeys()) {
            if (!cancelledKeys().isEmpty()) {
                LOGGER.debug("Removing [{}] cancelled {}", cancelledKeys().size(), cancelledKeys().size() == 1 ? "key" : "keys");
                keys.removeAll(cancelledKeys());
                selectedKeys.removeAll(cancelledKeys());
                cancelledKeys().clear();
            }
        }
    }

    private int performSelectOperation() throws IOException {
        LOGGER.debug("Selecting [{}] {}", keys.size(), keys.size() == 1 ? "key" : "keys");
        int updatedKeys = 0;

        for (SelectionKey key : Collections.unmodifiableSet(keys)) {
            ((HadronioSelectableChannel) key.channel()).select();

            if (selectKey((HadronioSelectionKey) key)) {
                updatedKeys++;
            }
        }

        LOGGER.debug("Finished selecting (updatedKeys: [{}])", updatedKeys);
        return updatedKeys;
    }

    private static final class FixedSelectionKeySet extends HashSet<SelectionKey> {

        public FixedSelectionKeySet() {
            super();
        }

        @Override
        public boolean add(final SelectionKey key) {
            throw new UnsupportedOperationException("Trying to add a key to a fixed set!");
        }

        @Override
        public boolean addAll(final Collection<? extends SelectionKey> keys) {
            throw new UnsupportedOperationException("Trying to add a key to a fixed set!");
        }

        private void addKey(final SelectionKey key) {
            super.add(key);
        }
    }
}
