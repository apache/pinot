package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.columnar.readers.DoubleColumnarReader;
import com.linkedin.pinot.core.indexsegment.columnar.readers.FloatColumnarReader;
import com.linkedin.pinot.core.indexsegment.columnar.readers.IntColumnarReader;
import com.linkedin.pinot.core.indexsegment.columnar.readers.LongColumnarReader;
import com.linkedin.pinot.core.indexsegment.columnar.readers.StringColumnarReader;


public class SimpleBlockValIterator implements BlockValIterator {

  private int _pos = 0;
  private long _size = 0;
  private final ColumnarReader _columnarReader;

  public SimpleBlockValIterator(ColumnarReader columnarReader, long size) {
    _columnarReader = columnarReader;
    _size = size;
  }

  @Override
  public int nextVal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int currentDocId() {
    return _pos;
  }

  @Override
  public int currentValId() {
    throw new UnsupportedOperationException();
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
  public String nextStringVal() {
    return _columnarReader.getStringValue(_pos++);
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
  public int nextDictVal() {
    return _columnarReader.getDictionaryId(_pos++);
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
}
