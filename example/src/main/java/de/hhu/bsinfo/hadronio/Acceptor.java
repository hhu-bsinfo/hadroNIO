package de.hhu.bsinfo.hadronio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverSocket;

    private SocketChannel socket;

    public Acceptor(Selector selector, ServerSocketChannel serverSocket) {
        this.selector = selector;
        this.serverSocket = serverSocket;
    }

    @Override
    public void run() {
        try {
            socket = serverSocket.accept();
            socket.configureBlocking(false);

            SelectionKey key = socket.register(selector, SelectionKey.OP_CONNECT);
            key.attach(new Handler(key, socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
