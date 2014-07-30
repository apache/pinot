package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;

public class ColumnarDataSource implements DataSource {

	String invertedIndexFile;
	String forwardIndexFile;
	String dictionaryFile;

	public ColumnarDataSource(String invertedIndexFile,
			String forwardIndexFile, String dictionaryFile) {
		this.invertedIndexFile = invertedIndexFile;
		this.forwardIndexFile = forwardIndexFile;
		this.dictionaryFile = dictionaryFile;
	}

	@Override
	public boolean open() {
		return false;
	}

	@Override
	public boolean close() {
		return false;
	}

	@Override
	public boolean setPredicate(Predicate predicate) {
		return false;
	}

	@Override
	public Block nextBlock() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Block nextBlock(BlockId BlockId) {
		// TODO Auto-generated method stub
		return null;
	}

}
