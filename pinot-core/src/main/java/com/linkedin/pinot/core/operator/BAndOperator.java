package com.linkedin.pinot.core.operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.linkedin.pinot.core.block.IntBlockDocIdSet;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Pairs;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.Pairs.IntPair;

public class BAndOperator implements Operator {

	private final Operator[] operators;

	public BAndOperator(Operator left, Operator right) {
		operators = new Operator[] { left, right };
	}

	public BAndOperator(Operator... operators) {
		this.operators = operators;
	}

	public BAndOperator(List<Operator> operators) {
		this.operators = new Operator[operators.size()];
		operators.toArray(this.operators);
	}

	@Override
	public boolean open() {
		for (Operator operator : operators) {
			operator.open();
		}
		return true;
	}

	@Override
	public boolean close() {
		for (Operator operator : operators) {
			operator.close();
		}
		return true;
	}

	@Override
	public Block nextBlock() {
		Block[] blocks = new Block[operators.length];
		int i = 0;
		boolean isAnyBlockEmpty = false;
		for (int srcId = 0; srcId < operators.length; srcId++) {
			Operator operator = operators[srcId];
			Block nextBlock = operator.nextBlock();
			if (nextBlock == null) {
				isAnyBlockEmpty = true;
			}
			blocks[i++] = nextBlock;
		}
		if (isAnyBlockEmpty) {
			return null;
		}
		return new AndBlock(blocks);
	}

	@Override
	public Block nextBlock(BlockId BlockId) {

		return null;
	}

}

class AndBlock implements Block {

	private final Block[] blocks;

	int[] intersection;

	public AndBlock(Block[] blocks) {
		this.blocks = blocks;
	}

	@Override
	public boolean applyPredicate(Predicate predicate) {
		return false;
	}

	@Override
	public BlockId getId() {
		return null;
	}

	@Override
	public int getIntValue(int docId) {
		return 0;
	}

	@Override
	public float getFloatValue(int docId) {
		return 0;
	}

	@Override
	public BlockMetadata getMetadata() {
		return null;
	}

	@Override
	public void resetBlock() {

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

		ArrayList<Integer> list = new ArrayList<Integer>();
		PriorityQueue<IntPair> queue = new PriorityQueue<IntPair>(
				blocks.length, Pairs.intPairComparator());
		BlockDocIdIterator blockDocIdSetIterators[] = new BlockDocIdIterator[blocks.length];

		// initialize
		for (int srcId = 0; srcId < blocks.length; srcId++) {
			Block block = blocks[srcId];
			BlockDocIdSet docIdSet = block.getBlockDocIdSet();
			blockDocIdSetIterators[srcId] = docIdSet.iterator();
			int nextDocId = blockDocIdSetIterators[srcId].next();
			queue.add(new IntPair(nextDocId, srcId));
		}
		int prevDocId = -1;
		int matchedCount = 0;
		while (queue.size() > 0) {
			IntPair pair = queue.poll();
			if (pair.getA() == prevDocId) {
				matchedCount = matchedCount + 1;
				if (matchedCount == blocks.length) {
					list.add(prevDocId);
				}
			} else {
				prevDocId = pair.getA();
				matchedCount = 1;
			}
			int nextDocId = blockDocIdSetIterators[pair.getB()].next();
			if (nextDocId > 0) {
				queue.add(new IntPair(nextDocId, pair.getB()));
			}
		}
		intersection = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			intersection[i] = list.get(i);
		}

		return new IntBlockDocIdSet(intersection);
	}
}

