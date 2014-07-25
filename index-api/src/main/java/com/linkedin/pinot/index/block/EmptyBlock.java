package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.BlockDocIdValueSet;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.BlockMetadata;
import com.linkedin.pinot.index.common.BlockValSet;
import com.linkedin.pinot.index.common.Predicate;

public class EmptyBlock implements Block {

	@Override
	public boolean applyPredicate(Predicate predicate) {
		return false;
	}

	@Override
	public BlockId getId() {
		return null;
	}

	@Override
	public BlockValSet getBlockValueSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlockDocIdValueSet getBlockDocIdValueSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlockDocIdSet getBlockDocIdSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlockMetadata getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getIntValue(int docId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloatValue(int docId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void resetBlock() {
		// TODO Auto-generated method stub
		
	}

}
