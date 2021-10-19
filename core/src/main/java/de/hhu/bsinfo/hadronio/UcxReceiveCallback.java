package de.hhu.bsinfo.hadronio;

public interface UcxReceiveCallback {

    void onSuccess(long tag);

    default void onError() {}
}
