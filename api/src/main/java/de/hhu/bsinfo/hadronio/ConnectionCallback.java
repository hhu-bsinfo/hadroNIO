package de.hhu.bsinfo.hadronio;

class ConnectionCallback implements UcxCallback {

    private final HadronioSocketChannel socket;

    ConnectionCallback(final HadronioSocketChannel socket) {
        this.socket = socket;
    }

    @Override
    public void onSuccess() {
        socket.onConnection(true);
    }

    @Override
    public void onError() {
        socket.onConnection(false);
    }
}
