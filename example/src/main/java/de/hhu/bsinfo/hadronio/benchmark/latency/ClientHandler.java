package de.hhu.bsinfo.hadronio.benchmark.latency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ClientHandler implements Handler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);

    private final SocketChannel socket;
    private final SelectionKey key;
    private final ByteBuffer messageBuffer;

    private boolean finished;

    public ClientHandler(final SocketChannel socket, final SelectionKey key, final ByteBuffer messageBuffer) {
        this.socket = socket;
        this.key = key;
        this.messageBuffer = messageBuffer;
        key.interestOps(SelectionKey.OP_READ);
    }

    public void reset() {
        finished = false;
        key.interestOps(SelectionKey.OP_READ);
    }

    public boolean isFinished() {
        return finished;
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
                finished = true;
                messageBuffer.flip();
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }
}
