package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class Handler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    private final SelectionKey key;
    private final SocketChannel socket;

    public Handler(SelectionKey key, SocketChannel socket) {
        this.key = key;
        this.socket = socket;
    }

    @Override
    public void run() {
        if (key.isConnectable()) {
            try {
                if (socket.finishConnect()) {
                    LOGGER.info("Connection established for key [{}]", key);
                }
            } catch (IOException e) {
                LOGGER.error("Failed to establish connection for [{}]", key);
            }

            key.interestOps(SelectionKey.OP_READ |  SelectionKey.OP_WRITE);
        }
    }
}
