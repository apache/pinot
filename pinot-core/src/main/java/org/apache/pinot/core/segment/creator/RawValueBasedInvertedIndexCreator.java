package org.apache.pinot.core.segment.creator;

import java.io.IOException;

public interface RawValueBasedInvertedIndexCreator extends InvertedIndexCreator {
    /**
     * For single-valued column, adds the integer value for the next document.
     */
    void add(int value);

    /**
     * For multi-valued column, add the integer values for the next document.
     */
    void add(int[] values, int length);

    /**
     * For single-valued column, adds the long value for the next document.
     */
    void add(long value);

    /**
     * For multi-valued column, add the long values for the next document.
     */
    void add(long[] values, int length);

    /**
     * For single-valued column, adds the float value for the next document.
     */
    void add(float value);

    /**
     * For multi-valued column, add the float values for the next document.
     */
    void add(float[] values, int length);

    /**
     * For single-valued column, adds the double value for the next document.
     */
    void add(double value);

    /**
     * For multi-valued column, add the double values for the next document.
     */
    void add(double[] values, int length);

    /**
     * Seals the index and flushes it to disk.
     */
    void seal()
            throws IOException;


}
