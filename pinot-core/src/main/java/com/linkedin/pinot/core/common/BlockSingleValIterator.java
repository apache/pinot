package com.linkedin.pinot.core.common;

/**
 * 
 *
 */
public abstract class BlockSingleValIterator implements BlockValIterator {


	char nextCharVal(){
		throw new UnsupportedOperationException();
	}

	public int nextIntVal(){
		throw new UnsupportedOperationException();
	}

	public float nextFloatVal(){
		throw new UnsupportedOperationException();
	}

	public long nextLongVal(){
		throw new UnsupportedOperationException();
	}

	public double nextDoubleVal(){
		throw new UnsupportedOperationException();
	}

	public byte[] nextBytesVal(){
		throw new UnsupportedOperationException();
	}


}
