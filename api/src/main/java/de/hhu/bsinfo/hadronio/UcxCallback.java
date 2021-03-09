package de.hhu.bsinfo.hadronio;

public interface UcxCallback {

    default void onSuccess() {}

    default void onError() {}
}
