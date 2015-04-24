/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.data.source.mv.block;

import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.utils.BitmapUtils;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class MultiValueBlockWithBitmapInvertedIndex implements Block {

  private final FixedBitCompressedMVForwardIndexReader mVReader;
  private final InvertedIndexReader invertedIndex;
  private final BlockId id;
  private final ImmutableDictionaryReader dictionary;
  private final ColumnMetadata columnMetadata;
  private List<Integer> filteredIds;
  private Predicate predicate;

  public MultiValueBlockWithBitmapInvertedIndex(BlockId id, FixedBitCompressedMVForwardIndexReader multiValueReader,
      InvertedIndexReader invertedIndex, ImmutableDictionaryReader dict, ColumnMetadata metadata) {
    this.invertedIndex = invertedIndex;
    mVReader = multiValueReader;
    this.id = id;
    dictionary = dict;
    columnMetadata = metadata;
  }

  public boolean hasDictionary() {
    return true;
  }

  public boolean hasInvertedIndex() {
    return columnMetadata.isHasInvertedIndex();
  }

  public boolean isSingleValued() {
    return columnMetadata.isSingleValue();
  }

  public int getMaxNumberOfMultiValues() {
    return columnMetadata.getMaxNumberOfMultiValues();
  }

  public ImmutableDictionaryReader getDictionary() {
    return dictionary;
  }

  public DataType getDataType() {
    return columnMetadata.getDataType();
  }

  @Override
  public BlockId getId() {
    return id;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    this.predicate = predicate;
    return false;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    /**
     * this method is expected to be only called when filter is set, otherwise block value set is called if
     * access to values are required
     */
    if (filteredIds == null) {
      return BlockUtils.getDummyBlockDocIdSet(columnMetadata.getTotalDocs());
    }

    return BlockUtils.getBLockDocIdSetBackedByBitmap(BitmapUtils.getOrBitmap(invertedIndex, filteredIds));
  }

  @Override
  public BlockValSet getBlockValueSet() {

    if (filteredIds != null) {
      return null;
    }

    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {

        return new BlockMultiValIterator() {
          private int counter = 0;

          @Override
          public int nextIntVal(int[] intArray) {
            return mVReader.getIntArray(counter++, intArray);
          }

          @Override
          public boolean skipTo(int docId) {
            if (docId >= mVReader.length()) {
              return false;
            }
            counter = docId;
            return true;
          }

          @Override
          public int size() {
            return mVReader.length();
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
          public boolean hasNext() {
            return (counter < mVReader.length());
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
    return null;
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
        return false;
      }

      @Override
      public boolean isSorted() {
        return columnMetadata.isSorted();
      }

      @Override
      public boolean isSingleValue() {
        return columnMetadata.isSingleValue();
      }

      @Override
      public boolean hasInvertedIndex() {
        return columnMetadata.isHasInvertedIndex();
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
      public ImmutableDictionaryReader getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return columnMetadata.getDataType();
      }
    };
  }
}
