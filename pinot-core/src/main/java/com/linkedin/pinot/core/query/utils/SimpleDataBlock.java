package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;


public class SimpleDataBlock implements Block {

  private final ColumnarReader _columnarReader;
  private final long _size;

  public SimpleDataBlock(ColumnarReader columnarReader, long size) {
    _columnarReader = columnarReader;
    _size = size;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          int _pos = 0;

          @Override
          public int skipTo(int targetDocId) {
            _pos = targetDocId;
            return next();
          }

          @Override
          public int next() {
            if (_pos >= _size) {
              return Constants.EOF;
            }
            return _pos++;
          }

          @Override
          public int currentDocId() {
            return _pos;
          }
        };
      }
    };
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new SimpleBlockValIterator(_columnarReader, _size);
      }

      @Override
      public DataType getValueType() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getStringValueAt(int dictionaryId) {
        return _columnarReader.getStringValue(dictionaryId);
      }

      @Override
      public long getLongValueAt(int dictionaryId) {
        return _columnarReader.getLongValue(dictionaryId);
      }

      @Override
      public int getIntValueAt(int dictionaryId) {
        return _columnarReader.getIntegerValue(dictionaryId);
      }

      @Override
      public float getFloatValueAt(int dictionaryId) {
        return _columnarReader.getFloatValue(dictionaryId);
      }

      @Override
      public double getDoubleValueAt(int dictionaryId) {
        return _columnarReader.getDoubleValue(dictionaryId);
      }

      @Override
      public int getDictionaryId(int docId) {
        return _columnarReader.getDictionaryId(docId);
      }

      @Override
      public int getDictionarySize() {
        return (int) _size;
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

}
