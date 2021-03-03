package de.hhu.bsinfo.hadronio.counter;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverSocket;

    public Acceptor(Selector selector, ServerSocketChannel serverSocket) {
        this.selector = selector;
        this.serverSocket = serverSocket;
    }

    @Override
    public void run() {
        try {
            final SocketChannel socket = serverSocket.accept();
            socket.configureBlocking(false);

            final SelectionKey key = socket.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            key.attach(new Handler(key, socket, 1000));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
