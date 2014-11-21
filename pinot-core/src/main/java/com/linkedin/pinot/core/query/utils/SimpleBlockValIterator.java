package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;


public class SimpleBlockValIterator extends BlockSingleValIterator {

  private int _pos = 0;
  private long _size = 0;
  private final ColumnarReader _columnarReader;

  public SimpleBlockValIterator(ColumnarReader columnarReader, long size) {
    _columnarReader = columnarReader;
    _size = size;
  }


  @Override
  public boolean reset() {
    _pos = 0;
    return true;
  }

  @Override
  public int nextIntVal() {
    return _columnarReader.getIntegerValue(_pos++);
  }

  @Override
  public float nextFloatVal() {
    return _columnarReader.getFloatValue(_pos++);
  }

  @Override
  public long nextLongVal() {
    return _columnarReader.getLongValue(_pos++);
  }

  @Override
  public double nextDoubleVal() {
    return _columnarReader.getDoubleValue(_pos++);
  }

  @Override
  public boolean hasNext() {
    return (_pos < _size);
  }

  @Override
  public int size() {
    return (int) _size;
  }


  @Override
  public DataType getValueType() {
    if (_columnarReader instanceof IntColumnarReader) {
      return DataType.INT;
    }
    if (_columnarReader instanceof FloatColumnarReader) {
      return DataType.FLOAT;
    }
    if (_columnarReader instanceof DoubleColumnarReader) {
      return DataType.DOUBLE;
    }
    if (_columnarReader instanceof StringColumnarReader) {
      return DataType.STRING;
    }
    if (_columnarReader instanceof LongColumnarReader) {
      return DataType.LONG;
    }
    throw new UnsupportedOperationException();
  }


  @Override
  public boolean skipTo(int docId) {
    // TODO Auto-generated method stub
    return false;
  }


  @Override
  public int currentDocId() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }
}
