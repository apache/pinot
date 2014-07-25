package com.linkedin.pinot.index.block;

import java.util.Arrays;

import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.Constants;

public class IntBlockDocIdIterator implements BlockDocIdIterator {

	private final int[] data;

	int currentIndex = -1;
	public IntBlockDocIdIterator(int[] data) {
		this.data = data;
	}

	/**
	 * throws arrayoutofboundsException is currentIndex is beyond the size of array
	 */
	@Override
	public int next() {
		if( currentIndex + 1 >= this.data.length){
			return Constants.EOF;
		}
		currentIndex = currentIndex + 1;
		return data[currentIndex];
	}

	@Override
	public int skipTo(int skipToDocId) {
		if( skipToDocId >= this.data.length){
			return Constants.EOF;
		}
		int index = Arrays.binarySearch(data, currentIndex, data.length,
				skipToDocId);
		return data[index];
	}

	@Override
	public int currentDocId() {
		return currentIndex;
	}


}
