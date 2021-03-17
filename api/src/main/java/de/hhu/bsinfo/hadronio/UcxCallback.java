package de.hhu.bsinfo.hadronio;

public interface UcxCallback {

    default void onSuccess(long tag) {}

    default void onError() {}
}
