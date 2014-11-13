package com.linkedin.pinot.core.block;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;

public class IntBlockValIterator extends BlockSingleValIterator {

	private final int[] data;

	int currentIndex = 0;

	public IntBlockValIterator(int[] data) {
		this.data = data;
	}

	/**
	 * throws arrayoutofboundsException is currentIndex is beyond the size of
	 * array
	 */
	@Override
	public int nextIntVal() {
		if (currentIndex >= this.data.length) {
			return Constants.EOF;
		}
		int ret = data[currentIndex];
		currentIndex = currentIndex + 1;
		return ret;
	}

	@Override
	public boolean reset() {
		currentIndex = 0;
		return true;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int size() {
		return data.length;
	}

	@Override
	public DataType getValueType() {
		return DataType.INT;
	}

	@Override
	public boolean skipTo(int docId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int currentDocId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return false;
	}

}
