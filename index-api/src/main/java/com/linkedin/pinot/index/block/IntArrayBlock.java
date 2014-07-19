package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.BlockDocIdValueSet;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.BlockMetadata;
import com.linkedin.pinot.index.common.BlockValSet;
import com.linkedin.pinot.index.common.Predicate;

/**
 * Uses array to represent data and index
 * 
 * @author kgopalak
 * 
 */
public class IntArrayBlock implements Block {

	private final int[] data;
	private final int[] filteredDocIds;
	private final BlockId blockId;

	public IntArrayBlock(BlockId id, int data[], int[] filteredDocIds) {
		this.blockId = id;
		this.data = data;
		this.filteredDocIds = filteredDocIds;
	}

	@Override
	public boolean applyPredicate(Predicate predicate) {
		return false;
	}

	@Override
	public int getIntValue(int docId) {
		return data[docId];
	}

	@Override
	public float getFloatValue(int docId) {
		return 0;
	}

	@Override
	public BlockId getId() {
		return blockId;
	}

	@Override
	public BlockValSet getBlockValueSet() {
		return null;
	}

	@Override
	public BlockDocIdValueSet getBlockDocIdValueSet() {
		return null;
	}

	@Override
	public BlockDocIdSet getBlockDocIdSet() {
		return new IntBlockDocIdSet(filteredDocIds);
	}

	@Override
	public BlockMetadata getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void resetBlock() {
		throw new UnsupportedOperationException(
				"reset block is not yet supported");
	}

}
