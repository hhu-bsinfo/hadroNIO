package de.hhu.bsinfo.hadronio;

public interface UcxCallback {

    void onSuccess(long localTag, long remoteTag);

    default void onError() {}
}
