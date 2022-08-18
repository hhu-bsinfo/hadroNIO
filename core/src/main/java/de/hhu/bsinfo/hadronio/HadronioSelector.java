package de.hhu.bsinfo.hadronio;

import de.hhu.bsinfo.hadronio.binding.UcxWorker;
import io.helins.linux.epoll.Epoll;
import io.helins.linux.epoll.EpollEvent;
import io.helins.linux.epoll.EpollEvents;
import org.agrona.collections.Int2ObjectHashMap;
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
    private final Epoll epoll = new Epoll();
    private final Int2ObjectHashMap<HadronioSelectableChannel> epollMap = new Int2ObjectHashMap<>();
    private final Object wakeupLock = new Object();
    private final Configuration.PollMethod pollMethod;
    private final int busyPollTimeout;

    private EpollEvents epollEvents;
    private boolean lastPollHadEvents = true;
    private boolean wakeupStatus = false;
    private boolean selectorClosed = false;

    HadronioSelector(final SelectorProvider selectorProvider) throws IOException {
        super(selectorProvider);
        final Configuration configuration = Configuration.getInstance();
        pollMethod = configuration.getPollMethod();
        busyPollTimeout = configuration.getBusyPollTimeoutNanos();
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
                        epoll.close();
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
                try {
                    final int channelDescriptor = ((HadronioSelectableChannel) channel).getWorker().getEventFileDescriptor();

                    final EpollEvent event = new EpollEvent()
                            .setFlags(new EpollEvent.Flags().set(EpollEvent.Flag.EPOLLIN).set(EpollEvent.Flag.EPOLLERR))
                            .setUserData(channelDescriptor);
                    epoll.add(channelDescriptor, event);
                    epollMap.put(channelDescriptor, (HadronioSelectableChannel) channel);
                } catch (IOException e) {
                    LOGGER.error("Unable to add file descriptor to epoll instance", e);
                }

                keys.add(key);
                epollEvents = new EpollEvents(keys.size());
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
        LOGGER.trace("Waking up worker");
        synchronized (wakeupLock) {
            wakeupStatus = true;
            for (final SelectionKey key : keys) {
                ((HadronioSelectableChannel) key.channel()).getWorker().interrupt();
            }
            wakeupLock.notifyAll();
        }

        return this;
    }

    private int select(final boolean blocking, final long timeout) throws IOException {
        if (selectorClosed) {
            throw new ClosedSelectorException();
        }

        LOGGER.trace("Starting select operation (blocking: [{}], timeout: [{}])", blocking, timeout);

        boolean keysAvailable = checkKeys(blocking, timeout);
        if (!keysAvailable) {
            return 0;
        }

        synchronized (this) {
            synchronized (keys) {
                synchronized (selectedKeys) {
                    boolean firstIteration = true;
                    pollWorkers(false, 0);

                    int updatedKeys = 0;
                    do {
                        removeCancelledKeys();

                        final int selectCount = performSelectOperation();
                        updatedKeys += selectCount;

                        if (!firstIteration) {
                            lastPollHadEvents = selectCount > 0;
                        }

                        removeCancelledKeys();
                        LOGGER.trace("Finished select iteration (blocking: [{}], keys: [{}], selectedKeys: [{}])", blocking, keys.size(), selectedKeys.size());

                        if (blocking && keys.size() > 0 && selectedKeys.size() == 0) {
                            pollWorkers(true, timeout * 1000000);

                            synchronized (wakeupLock) {
                                if (wakeupStatus) {
                                    wakeupStatus = false;
                                    break;
                                }
                            }
                        }

                        firstIteration = false;
                    } while (blocking && keys.size() > 0 && selectedKeys.size() == 0);

                    LOGGER.trace("Finished select operation (blocking: [{}], timeout: [{}], updatedKeys: [{}])", blocking, timeout, updatedKeys);
                    return updatedKeys;
                }
            }
        }
    }

    private void pollWorkers(final boolean blocking, final long timeoutNanos) throws IOException {
        if (!blocking) {
            busyPollWorkers(false, 0);
            return;
        }

        switch (pollMethod) {
            case BUSY_POLLING:
                busyPollWorkers(true, timeoutNanos);
                break;
            case EPOLL: {
                final boolean eventsPolled = drainWorkers();
                if (!eventsPolled) {
                    epollWorkers(timeoutNanos);
                }

                break;
            }
            case DYNAMIC: {
                long timeLeft = timeoutNanos;
                boolean eventsPolled = false;
                if (lastPollHadEvents) {
                    eventsPolled = busyPollWorkers(true, (busyPollTimeout < timeLeft || timeLeft == 0) ? busyPollTimeout : timeLeft);
                }

                if (timeoutNanos > 0) {
                    timeLeft -= busyPollTimeout;
                }

                synchronized (wakeupLock) {
                    if (wakeupStatus || (timeoutNanos > 0 && timeLeft <= 0)) {
                        lastPollHadEvents = wakeupStatus;
                        break;
                    }
                }

                if (!eventsPolled) {
                    eventsPolled = drainWorkers();
                    if (!eventsPolled) {
                        epollWorkers(timeLeft);
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("Illegal poll method '" + pollMethod + "'!");
        }
    }

    private boolean busyPollWorkers(final boolean blocking, final long timeoutNanos) {
            LOGGER.trace("Busy polling workers (blocking: [{}], timeout: [{} ns])", blocking, timeoutNanos);
            boolean eventsPolled = false;
            final long endTime = System.nanoTime() + timeoutNanos;

            do {
                for (final SelectionKey key : keys) {
                    final HadronioSelectableChannel channel = (HadronioSelectableChannel) key.channel();
                    eventsPolled |= channel.getWorker().progress();
                }

                if (!eventsPolled && timeoutNanos > 0 && System.nanoTime() > endTime) {
                    LOGGER.trace("Timeout of [{}] has been reached while polling workers", timeoutNanos);
                    break;
                }
            } while(blocking && !eventsPolled && !wakeupStatus);

            LOGGER.trace("Finished busy polling workers (eventsPolled: [{}])", eventsPolled);
            return eventsPolled;
    }

    private boolean epollWorkers(final long timeoutNanos) throws IOException {
        LOGGER.trace("Polling workers using epoll (timeout: [{} ns])", timeoutNanos);
        final int eventCount = epoll.wait(epollEvents, timeoutNanos <= 0 ? -1 : (int) (timeoutNanos / 1000000));
        LOGGER.trace("Epoll wait() finished (eventCount: [{}])", eventCount);

        boolean eventsPolled = false;
        for (int i = 0; i < eventCount; i++) {
            final HadronioSelectableChannel channel = epollMap.get((int) epollEvents.getEpollEvent(i).getUserData());
            if (channel == null) {
                continue;
            }

            final UcxWorker worker = channel.getWorker();
            worker.waitForEvents();

            boolean armed = false;
            do {
                try {
                    eventsPolled |= worker.drain();
                    worker.arm();
                    armed = true;
                } catch (Exception e) {
                    LOGGER.trace("Failed to arm worker");
                }
            } while (!armed);
        }

        LOGGER.trace("Finished polling workers using epoll (eventsPolled: [{}])", eventsPolled);
        return eventsPolled;
    }

    private boolean drainWorkers() {
        LOGGER.trace("Draining workers");
        boolean eventsPolled = false;
        for (final SelectionKey key : keys) {
            final UcxWorker worker = ((HadronioSelectableChannel) key.channel()).getWorker();
            if (worker.drain()) {
                eventsPolled = true;
            }
        }

        LOGGER.trace("Finished draining worker (eventsPolled: [{}])", eventsPolled);
        return eventsPolled;
    }

    private boolean selectKey(final HadronioSelectionKey key) {
        LOGGER.trace("Selecting key: [{}]", key);
        final int channelReadyOps = ((HadronioSelectableChannel) key.channel()).readyOps();
        final int readyOps = channelReadyOps & key.interestOps();
        LOGGER.trace("Selected channel (channelReadyOps: [{}], readyOps: [{}])", channelReadyOps, readyOps);

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

        LOGGER.trace("No keys are registered");
        if (!blocking) {
            return false;
        }

        synchronized (wakeupLock) {
            try {
                LOGGER.trace("Waiting for new keys (timeout: [{}])", timeout);
                wakeupLock.wait(timeout);
            } catch (InterruptedException e) {
                LOGGER.warn("Thread has been interrupted while waiting for keys to be registered");
                return false;
            }

            if (wakeupStatus) {
                LOGGER.trace("Selector has been interrupted by wakeup");
                wakeupStatus = false;
                return false;
            }
        }

        if (keys.isEmpty()) {
            LOGGER.trace("There are still no keys registered after wait() has returned");
            return false;
        }

        return true;
    }

    private void removeCancelledKeys() {
        synchronized (cancelledKeys()) {
            if (!cancelledKeys().isEmpty()) {
                LOGGER.trace("Removing [{}] cancelled {}", cancelledKeys().size(), cancelledKeys().size() == 1 ? "key" : "keys");
                for (final SelectionKey key : cancelledKeys()) {
                    try {
                        final int channelDescriptor = ((HadronioSelectableChannel) key.channel()).getWorker().getEventFileDescriptor();
                        epollMap.remove(channelDescriptor);
                        epoll.remove(channelDescriptor);
                    } catch (IOException e) {
                        LOGGER.error("Failed to remove file descriptor from epoll", e);
                    }
                }

                keys.removeAll(cancelledKeys());
                selectedKeys.removeAll(cancelledKeys());
                cancelledKeys().clear();
                epollEvents = keys.size() == 0 ? null : new EpollEvents(keys.size());
            }
        }
    }

    private int performSelectOperation() {
        LOGGER.trace("Selecting [{}] {}", keys.size(), keys.size() == 1 ? "key" : "keys");
        int updatedKeys = 0;

        for (SelectionKey key : Collections.unmodifiableSet(keys)) {
            ((HadronioSelectableChannel) key.channel()).select();

            if (selectKey((HadronioSelectionKey) key)) {
                updatedKeys++;
            }
        }

        LOGGER.trace("Finished selecting (updatedKeys: [{}])", updatedKeys);
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
