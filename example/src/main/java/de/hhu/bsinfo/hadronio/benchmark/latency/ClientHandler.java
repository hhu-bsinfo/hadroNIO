package de.hhu.bsinfo.hadronio.benchmark.latency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ClientHandler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);

    private final SocketChannel socket;
    private final SelectionKey key;
    private final ByteBuffer messageBuffer;

    private int remainingMessages;

    public ClientHandler(final SocketChannel socket, final SelectionKey key, final ByteBuffer messageBuffer, final int messageCount) {
        this.socket = socket;
        this.key = key;
        this.messageBuffer = messageBuffer;
        remainingMessages = messageCount;
    }

    @Override
    public void run() {
        if (key.isReadable()) {
            try {
                socket.read(messageBuffer);
            } catch (IOException e) {
                LOGGER.error("Failed to receive a message!");
            }

            if (!messageBuffer.hasRemaining()) {
                messageBuffer.flip();
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } else if (key.isWritable()) {
            try {
                socket.write(messageBuffer);
            } catch (IOException e) {
                LOGGER.error("Failed to send a message!");
            }

            if (!messageBuffer.hasRemaining()) {
                messageBuffer.flip();
                remainingMessages--;
                key.interestOps(SelectionKey.OP_READ);
            }
        }

        if (remainingMessages <= 0) {
            key.cancel();
        }
    }
}
