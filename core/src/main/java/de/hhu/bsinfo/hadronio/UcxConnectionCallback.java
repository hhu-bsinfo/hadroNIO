package de.hhu.bsinfo.hadronio;

public interface UcxConnectionCallback {

    void onSuccess(long localTag, long remoteTag);

    default void onError() {}
}
