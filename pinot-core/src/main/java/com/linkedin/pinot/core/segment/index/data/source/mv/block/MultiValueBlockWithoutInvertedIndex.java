package com.linkedin.pinot.core.segment.index.data.source.mv.block;

import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;
import com.linkedin.pinot.core.segment.index.data.source.DictionaryIdFilterUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class MultiValueBlockWithoutInvertedIndex implements Block {

  private final FixedBitCompressedMVForwardIndexReader indexReader;
  private final BlockId id;
  private final ImmutableDictionaryReader dictionary;
  private final ColumnMetadata columnMetadata;
  private Predicate predicate;

  public MultiValueBlockWithoutInvertedIndex(BlockId id, FixedBitCompressedMVForwardIndexReader multiValueReader,
      ImmutableDictionaryReader dict, ColumnMetadata metadata) {
    indexReader = multiValueReader;
    this.id = id;
    dictionary = dict;
    columnMetadata = metadata;
  }

  @Override
  public BlockId getId() {
    return id;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    this.predicate = predicate;
    return true;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    if (predicate == null) {
      return BlockUtils.getDummyBlockDocIdSet(columnMetadata.getTotalDocs());
    }

    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          private final List<Integer> filteredIds = DictionaryIdFilterUtils.filter(predicate, dictionary);
          int counter = -1;

          @Override
          public int skipTo(int targetDocId) {
            if (targetDocId >= columnMetadata.getTotalDocs()) {
              return Constants.EOF;
            }
            counter = targetDocId - 1;
            return next();
          }

          @Override
          public int next() {
            counter++;
            if (counter >= columnMetadata.getTotalDocs()) {
              return Constants.EOF;
            }
            while (counter < columnMetadata.getTotalDocs()) {
              int[] mval = new int[columnMetadata.getMaxNumberOfMultiValues()];
              int rlen = indexReader.getIntArray(counter, mval);
              for (int i = 0; i < rlen; i++) {
                if (filteredIds.contains(mval[i])) {
                  break;
                }
              }
              counter++;
            }
            return counter;
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public Object getRaw() {
        throw new UnsupportedOperationException("cannot get raw from forward index blocks");
      }
    };
  }

  @Override
  public BlockValSet getBlockValueSet() {
    if (predicate != null) {
      return null;
    }
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockMultiValIterator() {

          private int counter = 0;

          @Override
          public boolean skipTo(int docId) {
            if (counter >= columnMetadata.getTotalDocs()) {
              return false;
            }
            counter = docId;
            return false;
          }

          @Override
          public int size() {
            return 0;
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
          public int nextIntVal(int[] intArray) {
            return indexReader.getIntArray(counter++, intArray);
          }

          @Override
          public boolean hasNext() {
            return counter < columnMetadata.getTotalDocs();
          }

          @Override
          public DataType getValueType() {
            return columnMetadata.getDataType();
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public DataType getValueType() {
        return columnMetadata.getDataType();
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("get BlockDocIdValueSet not supported");
  }

  @Override
  public BlockMetadata getMetadata() {
    return new BlockMetadata() {

      @Override
      public int maxNumberOfMultiValues() {
        return columnMetadata.getMaxNumberOfMultiValues();
      }

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
        return false;
      }

      @Override
      public boolean hasInvertedIndex() {
        return false;
      }

      @Override
      public boolean hasDictionary() {
        return columnMetadata.hasDictionary();
      }

      @Override
      public int getStartDocId() {
        return 0;
      }

      @Override
      public int getSize() {
        return columnMetadata.getTotalDocs();
      }

      @Override
      public int getLength() {
        return columnMetadata.getTotalDocs();
      }

      @Override
      public int getEndDocId() {
        return columnMetadata.getTotalDocs() - 1;
      }

      @Override
      public Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return columnMetadata.getDataType();
      }
    };
  }

}
