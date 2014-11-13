package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.block.sets.utils.UnSortedBlockValSet;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntBlockValSet implements BlockValSet {

  final IntArray intArray;
  final Predicate p;
  final int start;
  final int end;
  final Dictionary<?> dictionary;
  final DataType type;

  public CompressedIntBlockValSet(IntArray intArray, Dictionary<?> dictionary, int start, int end, Predicate p,
      DataType type) {
    this.intArray = intArray;
    this.p = p;
    this.start = start;
    this.end = end;
    this.dictionary = dictionary;
    this.type = type;
  }

  @Override
  public BlockValIterator iterator() {

    if (p == null) {
      return UnSortedBlockValSet.getDefaultIterator(intArray, start, end);
    }

    switch (p.getType()) {
      case EQ:
        final int equalsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockValSet.getEqualityMatchIterator(equalsLookup, intArray, start, end);
      case NEQ:
        final int notEqualsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockValSet.getNoEqualsMatchIterator(notEqualsLookup, intArray, start, end);
      default:
        throw new UnsupportedOperationException("current I don't support predicate type : " + p.getType());
    }
  }

  @Override
  public DataType getValueType() {
    return type;
  }
/*
  @Override
  public int getIntValueAt(int dictionaryId) {
    return dictionary.getInteger(dictionaryId);
  }

  @Override
  public long getLongValueAt(int dictionaryId) {
    return dictionary.getLong(dictionaryId);
  }

  @Override
  public float getFloatValueAt(int dictionaryId) {
    return dictionary.getFloat(dictionaryId);
  }

  @Override
  public double getDoubleValueAt(int dictionaryId) {
    return dictionary.getDouble(dictionaryId);
  }

  @Override
  public String getStringValueAt(int dictionaryId) {
    return dictionary.getString(dictionaryId);
  }

  @Override
  public int getDictionaryId(int docId) {
    return intArray.getInt(docId);
  }

  @Override
  public int getDictionarySize() {
    return dictionary.size();
  }
  */
}
