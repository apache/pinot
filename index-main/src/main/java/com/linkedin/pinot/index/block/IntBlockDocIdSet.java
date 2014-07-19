package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdSet;

public class IntBlockDocIdSet implements BlockDocIdSet{

	private final int[] docIdArray;
	public IntBlockDocIdSet(int[] docIdArray){
		this.docIdArray = docIdArray;
		
	}
	@Override
	public BlockDocIdIterator iterator() {
		return new IntBlockDocIdIterator(docIdArray);
	}

}
