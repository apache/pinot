package com.linkedin.pinot.index.block;

import com.linkedin.pinot.index.common.BlockValIterator;
import com.linkedin.pinot.index.common.BlockValSet;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Predicate;

public class FakeBlockValSet implements BlockValSet {

	int[] vals;
	final Predicate p;
	int start;
	int end;

	public FakeBlockValSet(int[] vals, Predicate p) {
		this.vals = vals;
		this.p = p;
	}

	@Override
	public BlockValIterator iterator() {
		// TODO Auto-generated method stub
		return new BlockValIterator() {
			private int counter = 0;
			private int[] data = vals;

			@Override
			public int nextVal() {
				int ret = Constants.EOF;
				switch (p.getType()) {
				case EQ:
					while (counter < data.length) {
						if (vals[counter] == Integer
								.parseInt(p.getRhs().get(0))) {
							ret = counter;
						}
						counter++;
					}
					return ret;
				default:
					break;
				}
				return vals[counter];
			}

			@Override
			public int currentDocId() {
				return counter;
			}

			@Override
			public int currentValId() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public boolean reset() {
				// TODO Auto-generated method stub
				return false;
			}

		};
	}
}
