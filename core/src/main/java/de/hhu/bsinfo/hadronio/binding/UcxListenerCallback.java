package de.hhu.bsinfo.hadronio.binding;

@FunctionalInterface
public interface UcxListenerCallback {

    void onConnectionRequest(UcxConnectionRequest connectionRequest);
}
