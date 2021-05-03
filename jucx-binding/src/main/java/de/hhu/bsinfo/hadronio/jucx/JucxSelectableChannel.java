package de.hhu.bsinfo.hadronio.jucx;

import de.hhu.bsinfo.hadronio.UcxSelectableChannel;

import java.io.IOException;

public abstract class JucxSelectableChannel implements UcxSelectableChannel {

    private final JucxWorker worker;

    public JucxSelectableChannel(JucxWorker worker) {
        this.worker = worker;
    }

    protected JucxWorker getWorker() {
        return worker;
    }

    @Override
    public void pollWorker(boolean blocking) throws IOException {
        worker.poll(blocking);
    }
}
