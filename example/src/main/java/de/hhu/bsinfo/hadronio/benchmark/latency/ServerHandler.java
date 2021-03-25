package de.hhu.bsinfo.hadronio.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.LatencyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ServerHandler implements Handler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final SocketChannel socket;
    private final SelectionKey key;
    private final ByteBuffer messageBuffer;

    private boolean finished;

    public ServerHandler(final SocketChannel socket, final SelectionKey key, final ByteBuffer messageBuffer) {
        this.socket = socket;
        this.key = key;
        this.messageBuffer = messageBuffer;
        key.interestOps(SelectionKey.OP_WRITE);
    }

    public void reset() {
        finished = false;
        key.interestOps(SelectionKey.OP_WRITE);
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void run() {
        if (key.isWritable()) {
            try {
                socket.write(messageBuffer);
            } catch (IOException e) {
                LOGGER.error("Failed to send a message!");
            }

            if (!messageBuffer.hasRemaining()) {
                messageBuffer.flip();
                key.interestOps(SelectionKey.OP_READ);
            }
        } else if (key.isReadable()) {
            try {
                socket.read(messageBuffer);
            } catch (IOException e) {
                LOGGER.error("Failed to receive a message!");
            }

            if (!messageBuffer.hasRemaining()) {
                finished = true;
                messageBuffer.flip();
                key.interestOps(SelectionKey.OP_WRITE);
            }
        }
    }
}
