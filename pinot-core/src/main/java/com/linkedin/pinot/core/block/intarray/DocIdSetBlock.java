package com.linkedin.pinot.core.block.intarray;

import java.util.Arrays;

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
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class DocIdSetBlock implements Block {

  private final IndexSegment _indexSegment;
  private final int[] _docIdSet;
  private final int _searchableLength;

  public DocIdSetBlock(IndexSegment indexSegment, int[] docIdSet, int searchableLength) {
    _indexSegment = indexSegment;
    _docIdSet = docIdSet;
    _searchableLength = searchableLength;
  }

  public int[] getDocIdSet() {
    return _docIdSet;
  }

  public int getSearchableLength() {
    return _searchableLength;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return true;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {
      @Override
      public BlockValIterator iterator() {
        return new BlockValIterator() {
          int _pos = 0;

          @Override
          public int size() {
            return _searchableLength;
          }

          @Override
          public boolean reset() {
            _pos = 0;
            return true;
          }

          @Override
          public int nextVal() {
            return _pos++;
          }

          @Override
          public String nextStringVal() {
            return "" + (_pos++);
          }

          @Override
          public long nextLongVal() {
            return _pos++;
          }

          @Override
          public int nextIntVal() {
            return _pos++;
          }

          @Override
          public float nextFloatVal() {
            return _pos++;
          }

          @Override
          public double nextDoubleVal() {
            return _pos++;
          }

          @Override
          public int nextDictVal() {
            return _pos++;
          }

          @Override
          public boolean hasNext() {
            return _pos < _searchableLength;
          }

          @Override
          public int currentValId() {
            return _pos;
          }

          @Override
          public int currentDocId() {
            return _pos;
          }

          @Override
          public DataType getValueType() {
            return DataType.INT;
          }
        };
      }

      @Override
      public DataType getValueType() {
        return DataType.INT;
      }

      @Override
      public int getDictionaryId(int docId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public int getIntValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public long getLongValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public float getFloatValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public double getDoubleValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public String getStringValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public int getDictionarySize() {
        // TODO Auto-generated method stub
        return 0;
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
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
            _pos = Arrays.binarySearch(_docIdSet, targetDocId);
            if (_pos < 0) {
              _pos = (_pos + 1) * -1;
            }
            return _docIdSet[_pos];
          }

          @Override
          public int next() {
            if (_pos == _searchableLength) {
              return Constants.EOF;
            }
            return _docIdSet[_pos++];
          }

          @Override
          public int currentDocId() {
            return _docIdSet[_pos];
          }
        };
      }
    };
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

}
