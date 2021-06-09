package de.hhu.bsinfo.hadronio;

public interface UcxSendCallback {

    void onSuccess();

    default void onError() {}
}
