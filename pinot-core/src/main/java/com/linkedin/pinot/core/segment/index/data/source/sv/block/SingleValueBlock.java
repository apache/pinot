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

import java.util.Arrays;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
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
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 15, 2014
 */

public class SingleValueBlock implements Block {

  private final FixedBitCompressedSVForwardIndexReader sVReader;
  private final ImmutableRoaringBitmap filteredDocIdsBitMap;
  private final BlockId id;
  private final ImmutableDictionaryReader dictionary;
  private final ColumnMetadata columnMetadata;

  public SingleValueBlock(BlockId id, FixedBitCompressedSVForwardIndexReader singleValueReader,
      ImmutableRoaringBitmap filteredtBitmap, ImmutableDictionaryReader dict, ColumnMetadata columnMetadata) {
    filteredDocIdsBitMap = filteredtBitmap;
    sVReader = singleValueReader;
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
    return false;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    /**
     * this method is expected to be only called when filter is set, otherwise block value set is called if
     * access to values are required
     */
    if (filteredDocIdsBitMap == null) {
      return BlockUtils.getDummyBlockDocIdSet(columnMetadata.getTotalDocs());
    }
    return BlockUtils.getBLockDocIdSetBackedByBitmap(filteredDocIdsBitMap);
  }

  private BlockValSet returnBlockValueSetBackedByFwdIndex() {
    return new BlockValSet() {
      @Override
      public BlockValIterator iterator() {

        return new BlockSingleValIterator() {
          private int counter = 0;

          @Override
          public boolean skipTo(int docId) {
            if (docId >= sVReader.getLength()) {
              return false;
            }

            counter = docId;

            return true;
          }

          @Override
          public int size() {
            return sVReader.getLength();
          }

          @Override
          public int nextIntVal() {
            if (counter >= sVReader.getLength()) {
              return Constants.EOF;
            }
            return sVReader.getInt(counter++);
          }

          @Override
          public boolean reset() {
            counter = 0;
            return true;
          }

          @Override
          public boolean next() {
            // TODO Auto-generated method stub
            return false;
          }

          @Override
          public boolean hasNext() {
            return (counter < sVReader.getLength());
          }

          @Override
          public DataType getValueType() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public DataType getValueType() {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }

  @Override
  public BlockValSet getBlockValueSet() {
    if (filteredDocIdsBitMap == null) {
      return returnBlockValueSetBackedByFwdIndex();
    }

    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
          private final int[] docIds = filteredDocIdsBitMap.toArray();
          private int counter = 0;

          @Override
          public boolean skipTo(int docId) {
            int entry = Arrays.binarySearch(docIds, docId);
            if (entry < 0) {
              entry *= -1;
            }

            if (entry >= docIds.length) {
              return false;
            }

            counter = entry;
            return true;
          }

          @Override
          public int size() {
            return docIds.length;
          }

          @Override
          public int nextIntVal() {
            if (counter >= docIds.length) {
              return Constants.EOF;
            }
            return sVReader.getInt(docIds[counter++]);
          }

          @Override
          public boolean reset() {
            counter = 0;
            return true;
          }

          @Override
          public boolean next() {
            counter++;
            return counter < docIds.length;
          }

          @Override
          public boolean hasNext() {
            return (counter < docIds.length);
          }

          @Override
          public DataType getValueType() {
            return null;
          }

          @Override
          public int currentDocId() {
            return docIds[counter];
          }
        };
      }

      @Override
      public DataType getValueType() {
        // TODO Auto-generated method stub
        return null;
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
      public boolean isSparse() {
        return false;
      }

      @Override
      public boolean isSorted() {
        return columnMetadata.isSorted();
      }

      @Override
      public boolean hasInvertedIndex() {
        return columnMetadata.isHasInvertedIndex();
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
      public boolean hasDictionary() {
        return true;
      }

      @Override
      public boolean isSingleValue() {
        return columnMetadata.isSingleValue();
      }

      @Override
      public Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public int maxNumberOfMultiValues() {
        return columnMetadata.getMaxNumberOfMultiValues();
      }

      @Override
      public DataType getDataType() {
        return columnMetadata.getDataType();
      }
    };
  }
}
