package de.hhu.bsinfo.hadronio.util;

import java.util.PrimitiveIterator;
import java.util.Random;

public class TagGenerator {

    private static final PrimitiveIterator.OfInt tagIterator = new Random().ints(0, Integer.MAX_VALUE).distinct().iterator();

    private TagGenerator() {}

    public static int generateTag() {
        return tagIterator.nextInt();
    }
}
