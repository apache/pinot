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
package com.linkedin.pinot.core.segment.index.data.source.sv.block;

import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;
import com.linkedin.pinot.core.segment.index.data.source.DictionaryIdFilterUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class SingleValueBlockWithoutInvertedIndex implements Block {

  private final FixedBitCompressedSVForwardIndexReader indexReader;
  private final BlockId id;
  private final ImmutableDictionaryReader dictionary;
  private final ColumnMetadata columnMetadata;
  private Predicate predicate;

  public SingleValueBlockWithoutInvertedIndex(BlockId id, FixedBitCompressedSVForwardIndexReader singleValueReader,
      ImmutableDictionaryReader dict, ColumnMetadata columnMetadata) {
    indexReader = singleValueReader;
    this.id = id;
    dictionary = dict;
    this.columnMetadata = columnMetadata;
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

    final List<Integer> filteredIds = DictionaryIdFilterUtils.filter(predicate, dictionary);

    return new BlockDocIdSet() {
      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
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
              if (filteredIds.contains(indexReader.getInt(counter))) {
                return counter;
              }
              counter++;
            }
            return Constants.EOF;
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
        return new BlockSingleValIterator() {
          private int counter = 0;

          @Override
          public boolean skipTo(int docId) {
            if (docId >= columnMetadata.getTotalDocs()) {
              return false;
            }
            counter = docId;
            return true;
          }

          @Override
          public int size() {
            return columnMetadata.getTotalDocs();
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
            if (!hasNext()) {
              return Constants.EOF;
            }
            return indexReader.getInt(counter++);
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
        return -1;
      }

      @Override
      public boolean isSparse() {
        return true;
      }

      @Override
      public boolean isSorted() {
        return columnMetadata.isSorted();
      }

      @Override
      public boolean isSingleValue() {
        return true;
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
