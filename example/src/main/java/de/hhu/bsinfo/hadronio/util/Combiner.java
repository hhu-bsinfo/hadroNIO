package de.hhu.bsinfo.hadronio.util;

public interface Combiner {

    void addResult(final Result newResult);

    Result getCombinedResult();
}
