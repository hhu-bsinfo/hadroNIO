package de.hhu.bsinfo.hadronio.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Stack;

public class ResourceHandler implements Closeable {

    private final Stack<Closeable> resources = new Stack<>();

    public void addResource(Closeable resource) {
        resources.push(resource);
    }

    @Override
    public void close() throws IOException {
        while (!resources.isEmpty()) {
            try {
                resources.pop().close();
            } catch (Exception e) {
                throw new IOException("Failed to close resources!", e);
            }
        }
    }
}
