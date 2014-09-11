package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;


public class ColumnarReaderBlockValIterator implements BlockValIterator {

  private final ColumnarReader _columnarReader;
  private final int[] _docIdSet;
  private int _position = 0;
  private final int _searchableLength;

  public ColumnarReaderBlockValIterator(ColumnarReader columnarReader, int[] docIdSet, int searchableLength) {
    _searchableLength = searchableLength;
    _columnarReader = columnarReader;
    _docIdSet = docIdSet;
    _position = 0;
  }

  @Override
  public int nextVal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int currentDocId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int currentValId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean reset() {
    _position = 0;
    return true;
  }

  @Override
  public int nextIntVal() {
    return _columnarReader.getIntegerValue(_docIdSet[_position++]);
  }

  @Override
  public float nextFloatVal() {
    return _columnarReader.getFloatValue(_docIdSet[_position++]);
  }

  @Override
  public long nextLongVal() {
    return _columnarReader.getLongValue(_docIdSet[_position++]);
  }

  @Override
  public double nextDoubleVal() {
    return _columnarReader.getDoubleValue(_docIdSet[_position++]);
  }

  @Override
  public String nextStringVal() {
    return _columnarReader.getStringValue(_docIdSet[_position++]);
  }

  @Override
  public boolean hasNext() {
    return (_position < _searchableLength);
  }

  @Override
  public int size() {
    return _searchableLength;
  }

}
