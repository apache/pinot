package com.linkedin.pinot.core.common;

/**
 *
 *
 */
public abstract class BlockMultiValIterator implements BlockValIterator {

  public int nextCharVal(int[] charArray) {
    throw new UnsupportedOperationException();
  }

  public int nextIntVal(int[] intArray) {
    throw new UnsupportedOperationException();
  }

  public int nextLongVal(long[] longArray) {
    throw new UnsupportedOperationException();
  }

  public int nextFloatVal(float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  public int nextDoubleVal(double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  public byte[][] nextBytesArrayVal(byte[][] bytesArrays) {
    throw new UnsupportedOperationException();

  }

}
