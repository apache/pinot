package com.linkedin.pinot.core.common;

/**
 * 
 *
 */
public abstract class BlockMultiValIterator implements BlockValIterator {

	int nextCharVal(int[] charArray) {
		throw new UnsupportedOperationException();
	}

	int nextIntVal(int[] intArray) {
		throw new UnsupportedOperationException();
	}

	int nextLongVal(long[] longArray) {
		throw new UnsupportedOperationException();
	}

	int nextFloatVal(float[] floatArray) {
		throw new UnsupportedOperationException();
	}

	int nextDoubleVal(double[] doubleArray) {
		throw new UnsupportedOperationException();
	}

	byte[][] nextBytesArrayVal(byte[][] bytesArrays) {
		throw new UnsupportedOperationException();

	}

}
