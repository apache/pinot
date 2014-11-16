package com.linkedin.pinot.core.block.sets.utils;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;

public class UnSortedBlockValSet {

	/**
	 * 
	 * @param values
	 * @return
	 */
	public static BlockValIterator getDefaultIterator(final IntArray intArray,
			final int start, final int end) {
		return new BlockSingleValIterator() {
			int counter = start;

			@Override
			public boolean reset() {
				counter = start;
				return true;
			}

			@Override
			public int nextIntVal() {
				if (counter < end) {
					return intArray.getInt(counter++);
				}
				return Constants.EOF;
			}

			@Override
			public int currentDocId() {
				return counter;
			}

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public int size() {
				// TODO Auto-generated method stub
				return 0;
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
			public boolean next() {
				return false;
			}

		};
	}

	/**
	 * 
	 * @param start
	 * @param end
	 * @param values
	 * @return
	 */
	public static BlockValIterator getRangeMatchIterator(
			final IntArray intArray, final int start, final int end,
			final int rangeStart, final int rangeEnd) {
		return new BlockSingleValIterator() {
			int counter = start;

			@Override
			public boolean reset() {
				counter = start;
				return true;
			}

			@Override
			public int nextIntVal() {
				while (counter < end) {
					final int val = intArray.getInt(counter);
					if ((val >= start) & (val <= end)) {
						counter++;
						return val;
					}
					counter++;
				}
				return Constants.EOF;
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
			public boolean next() {
				// TODO Auto-generated method stub
				return false;
			}
			@Override
			public int currentDocId() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public int size() {
				// TODO Auto-generated method stub
				return 0;
			}
		};
	}

	/**
	 * Currently not taking dictionary into account, in reality there will be a
	 * dictionary
	 * 
	 * @param valueToLookup
	 * @param values
	 * @return
	 */
	public static BlockValIterator getNoEqualsMatchIterator(
			final int valueToLookup, final IntArray intArray, final int start,
			final int end) {
		return new BlockSingleValIterator() {
			int counter = start;

			@Override
			public boolean reset() {
				counter = start;
				return true;
			}

			@Override
			public int nextIntVal() {
				while (counter < end) {
					final int val = intArray.getInt(counter);
					if (valueToLookup != val) {
						counter++;
						return val;
					}
					counter++;
				}
				return Constants.EOF;
			}

			@Override
			public int currentDocId() {
				return counter;
			}

			@Override
			public int size() {
				// TODO Auto-generated method stub
				return 0;
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
			public boolean next() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

		};
	}

	/**
	 * Currently not taking dictionary into account, in reality there will be a
	 * dictionary
	 * 
	 * @param valueToLookup
	 * @param values
	 * @return
	 */
	public static BlockValIterator getEqualityMatchIterator(
			final int valueToLookup, final IntArray intArray, final int start,
			final int end) {
		return new BlockSingleValIterator() {
			int counter = start;

			@Override
			public boolean reset() {
				counter = start;
				return true;
			}

			@Override
			public int nextIntVal() {
				while (counter < end) {
					final int val = intArray.getInt(counter);
					if (valueToLookup == val) {
						counter++;
						return val;
					}
					counter++;
				}
				return Constants.EOF;
			}

			@Override
			public int currentDocId() {
				return counter;
			}

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public int size() {
				return intArray.size();
			}

			@Override
			public DataType getValueType() {
				return DataType.INT;
			}

			@Override
			public boolean skipTo(int docId) {
				counter = docId;
				return hasNext();
			}

			@Override
			public boolean next() {
				return false;
			}
		};
	}
}
