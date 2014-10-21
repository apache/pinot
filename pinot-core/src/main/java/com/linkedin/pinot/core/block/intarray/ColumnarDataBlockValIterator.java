package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;


public class ColumnarDataBlockValIterator implements BlockValIterator {

  private final int[] _docIdSet;
  private final BlockValSet _blockValSet;
  private final int _searchableLength;
  private int _position = 0;

  public ColumnarDataBlockValIterator(BlockValSet blockValSet, int[] docIdSet, int searchableLength) {
    _blockValSet = blockValSet;
    _docIdSet = docIdSet;
    _searchableLength = searchableLength;
  }

  @Override
  public int size() {
    return _searchableLength;
  }

  @Override
  public boolean reset() {
    _position = 0;
    return true;
  }

  @Override
  public int nextVal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nextStringVal() {
    return _blockValSet.getStringValueAt(_blockValSet.getDictionaryId(_docIdSet[_position++]));
  }

  @Override
  public long nextLongVal() {
    return _blockValSet.getLongValueAt(_blockValSet.getDictionaryId(_docIdSet[_position++]));
  }

  @Override
  public int nextIntVal() {
    return _blockValSet.getIntValueAt(_blockValSet.getDictionaryId(_docIdSet[_position++]));
  }

  @Override
  public float nextFloatVal() {
    return _blockValSet.getFloatValueAt(_blockValSet.getDictionaryId(_docIdSet[_position++]));
  }

  @Override
  public double nextDoubleVal() {
    return _blockValSet.getDoubleValueAt(_blockValSet.getDictionaryId(_docIdSet[_position++]));
  }

  @Override
  public int nextDictVal() {
    return _blockValSet.getDictionaryId(_docIdSet[_position++]);
  }

  @Override
  public boolean hasNext() {
    return (_position < _searchableLength);
  }

  @Override
  public int currentValId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int currentDocId() {
    throw new UnsupportedOperationException();
  }

}
