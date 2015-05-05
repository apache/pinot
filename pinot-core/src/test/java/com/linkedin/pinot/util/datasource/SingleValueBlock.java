package com.linkedin.pinot.util.datasource;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class SingleValueBlock implements Block {

  private final int[] dictionary;
  final int[] values;

  public SingleValueBlock(final int[] dicIds, final int[] values) {
    this.dictionary = dicIds;
    this.values = values;
  }

  @Override
  public BlockId getId() {
    return new BlockId(0);
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {

        return new BlockSingleValIterator() {
          int counter = 0;

          @Override
          public boolean skipTo(int docId) {
            if (docId >= values.length) {
              return false;
            }
            counter = docId;
            return true;
          }

          @Override
          public int size() {
            return values.length;
          }

          @Override
          public boolean reset() {
            counter = 0;
            return true;
          }

          @Override
          public boolean next() {
            return false;
          }

          @Override
          public int nextIntVal() {
            return values[counter++];
          }

          @Override
          public boolean hasNext() {
            return counter < values.length;
          }

          @Override
          public DataType getValueType() {
            return DataType.INT;
          }

          @Override
          public int currentDocId() {
            // TODO Auto-generated method stub
            return 0;
          }
        };
      }

      @Override
      public DataType getValueType() {
        return DataType.INT;
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {

    return new BlockMetadata() {
      @Override
      public boolean isSparse() {
        return true;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return true;
      }

      @Override
      public boolean hasInvertedIndex() {
        return true;
      }

      @Override
      public boolean hasDictionary() {
        return true;
      }

      @Override
      public int getStartDocId() {
        return 0;
      }

      @Override
      public int getSize() {
        return values.length;
      }

      @Override
      public int getLength() {
        return values.length;
      }

      @Override
      public int getEndDocId() {
        return values.length - 1;
      }

      @Override
      public Dictionary getDictionary() {
        try {
          return new IntArrayBackedImmutableDictionary(null, dictionary.length, 1, true, dictionary);
        } catch (Exception e) {

        }
        return null;
      }

      @Override
      public DataType getDataType() {
        return DataType.INT;
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        // TODO Auto-generated method stub
        return 0;
      }
    };
  }
}
