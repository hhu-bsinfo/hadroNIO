package de.hhu.bsinfo.hadronio.benchmark.latency;

import de.hhu.bsinfo.hadronio.util.LatencyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ServerHandler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final SocketChannel socket;
    private final SelectionKey key;
    private final ByteBuffer messageBuffer;
    private final LatencyResult result;

    private int remainingMessages;

    public ServerHandler(final SocketChannel socket, final SelectionKey key, final ByteBuffer messageBuffer, final int messageCount, final LatencyResult result) {
        this.socket = socket;
        this.key = key;
        this.messageBuffer = messageBuffer;
        this.result = result;
        remainingMessages = messageCount;
    }

    @Override
    public void run() {
        if (key.isWritable()) {
            if (messageBuffer.position() == 0) {
                result.startSingleMeasurement();
            }

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
                result.stopSingleMeasurement();
                messageBuffer.flip();
                remainingMessages--;
                key.interestOps(SelectionKey.OP_WRITE);
            }
        }

        if (remainingMessages <= 0) {
            key.cancel();
        }
    }
}
