package com.linkedin.pinot.index.datasource;

import java.util.Arrays;

import com.linkedin.pinot.index.block.EmptyBlock;
import com.linkedin.pinot.index.block.IntArrayBlock;
import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.operator.DataSource;

public class MultiBlockDataSource implements DataSource {

	private Predicate predicate;
	private int[] data;
	private int[][] invertedIndex;
	private int[] dictionary;
	private Block currentBlock;
	private int maxBlockSize = 1;
	private int currentBlockId = 0;

	/**
	 * 
	 * @param columnData
	 * @param invertedIndex
	 *            posting list for each value. The actual value can be found
	 *            from the dictionary
	 * @param dictionary
	 */
	public MultiBlockDataSource(int[] columnData, int[][] invertedIndex,
			int[] dictionary) {
		this.data = columnData;
		this.invertedIndex = invertedIndex;
		this.dictionary = dictionary;
	}

	@Override
	public boolean open() {
		switch (this.predicate.getType()) {
		case EQ:
			BlockId id = new BlockId(0);
			int key = new Integer(predicate.getRhs().get(0));
			int index = Arrays.binarySearch(dictionary, key);
			if (index < 0) {
				this.currentBlock = new EmptyBlock();
			} else {
				int[] filteredDocs = invertedIndex[index];
				this.currentBlock = new IntArrayBlock(id, data, filteredDocs);
			}
			break;
		default:
			break;
		}
		return true;
	}

	@Override
	public Block nextBlock() {
		if (currentBlockId + 1 >= maxBlockSize) {
			return null;
		}
		Block ret = this.currentBlock;
		currentBlockId = currentBlockId + 1;
		return ret;
	}

	@Override
	public Block nextBlock(BlockId blockId) {
		if (blockId.getId() < maxBlockSize) {
			currentBlockId = maxBlockSize;
			return nextBlock();
		} else {
			return new EmptyBlock();
		}
	}

	@Override
	public boolean close() {
		return true;
	}

	@Override
	public boolean setPredicate(Predicate predicate) {
		this.predicate = predicate;
		return true;
	}

}
