package de.hhu.bsinfo.hadronio.binding;

@FunctionalInterface
public interface UcxReceiveCallback {

    void onMessageReceived(long tag);
}
