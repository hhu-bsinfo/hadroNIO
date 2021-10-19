package de.hhu.bsinfo.hadronio;

class ConnectionCallback implements UcxConnectionCallback {

    private final HadronioSocketChannel socket;

    ConnectionCallback(final HadronioSocketChannel socket) {
        this.socket = socket;
    }

    @Override
    public void onSuccess(long localTag, long remoteTag) {
        socket.onConnection(true, localTag, remoteTag);
    }

    @Override
    public void onError() {
        socket.onConnection(false, -1, -1);
    }
}
