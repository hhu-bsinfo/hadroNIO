package de.hhu.bsinfo.hadronio.counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class Handler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    private final SelectionKey key;
    private final SocketChannel socket;
    private final int counterLimit;

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(Integer.BYTES);

    private int sendCounter;
    private int receiveCounter;

    public Handler(SelectionKey key, SocketChannel socket, int counterLimit) {
        this.key = key;
        this.socket = socket;
        this.counterLimit = counterLimit;
    }

    public void runBlocking() {
        while (sendCounter < counterLimit || receiveCounter < counterLimit) {
            if (sendCounter < counterLimit) {
                write();
            }

            if (receiveCounter < counterLimit) {
                read();
            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted unexpectedly", e);
        }
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

            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }

        if (key.isWritable() && sendCounter < counterLimit) {
            write();
        }

        if (key.isReadable() && receiveCounter < counterLimit) {
            read();
        }

        if (sendCounter >= counterLimit && receiveCounter >= counterLimit) {
            key.cancel();
        }
    }

    private void write() {
        if (sendBuffer.position() == 0) {
            LOGGER.info("Sending [{}]", ++sendCounter);

            sendBuffer.putInt(sendCounter);
            sendBuffer.rewind();
        }

        try {
            socket.write(sendBuffer);
        } catch (IOException e) {
            LOGGER.error("Unable to write to SocketChannel", e);
        }

        if (sendBuffer.hasRemaining()) {
            LOGGER.debug("Could not write all bytes at once! Remaining bytes: [{}]", sendBuffer.remaining());
            return;
        }

        sendBuffer.clear();
    }

    private void read() {
        try {
            socket.read(receiveBuffer);
        } catch (IOException e) {
            LOGGER.error("Unable to read from SocketChannel", e);
        }

        if (receiveBuffer.hasRemaining()) {
            LOGGER.debug("Could not read all bytes at once! Remaining bytes: [{}]", receiveBuffer.remaining());
            return;
        }

        receiveBuffer.flip();
        final int counter = receiveBuffer.getInt();

        if (counter != receiveCounter + 1) {
            LOGGER.warn("Counter jump from [{}] to [{}] detected!", receiveCounter, counter);
            System.exit(1);
        }

        receiveCounter = counter;
        LOGGER.info("Received [{}]", receiveCounter);

        receiveBuffer.clear();
    }
}
