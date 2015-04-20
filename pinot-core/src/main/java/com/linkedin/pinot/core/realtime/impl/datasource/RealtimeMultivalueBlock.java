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
package com.linkedin.pinot.core.realtime.impl.datasource;

import java.nio.ByteBuffer;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RealtimeMultivalueBlock implements Block {

  private final MutableRoaringBitmap filteredBitmap;
  private final FieldSpec spec;
  private final MutableDictionaryReader dictionary;
  private final String columnName;
  private final int docIdSearchableOffset;
  private final Schema schema;
  private Predicate p;
  private final int maxNumberOfMultiValuesMap;
  private final RealtimeDimensionsSerDe dimeSerDe;

  private final ByteBuffer[] dimBuffs;

  public RealtimeMultivalueBlock(FieldSpec spec, MutableDictionaryReader dictionary,
      MutableRoaringBitmap filteredDocids, String columnName, int docIdOffset, Schema schema,
      int maxNumberOfMultiValuesMap, RealtimeDimensionsSerDe dimeSerDe, ByteBuffer[] dims) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.filteredBitmap = filteredDocids;
    this.columnName = columnName;
    this.docIdSearchableOffset = docIdOffset;
    this.schema = schema;
    this.maxNumberOfMultiValuesMap = maxNumberOfMultiValuesMap;
    this.dimeSerDe = dimeSerDe;
    this.dimBuffs = dims;
  }

  @Override
  public BlockId getId() {
    return null;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    this.p = predicate;
    return true;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    if (this.p != null) {
      return BlockUtils.getBLockDocIdSetBackedByBitmap(filteredBitmap);
    }

    return BlockUtils.getDummyBlockDocIdSet(docIdSearchableOffset);
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockMultiValIterator() {
          private int counter = 0;
          private int max = docIdSearchableOffset;

          @Override
          public boolean skipTo(int docId) {
            if (docId > max) {
              return false;
            }
            counter = docId;
            return true;
          }

          @Override
          public int size() {
            return max;
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
            if (counter >= max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = dimBuffs[counter++];
            int[] temp = dimeSerDe.deSerializeAndReturnDicIdsFor(columnName, rawData);
            System.arraycopy(temp, 0, intArray, 0, temp.length);
            return temp.length;
          }

          @Override
          public boolean hasNext() {
            return (counter <= max);
          }

          @Override
          public DataType getValueType() {
            return spec.getDataType();
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public DataType getValueType() {
        return spec.getDataType();
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
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return false;
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
        return docIdSearchableOffset;
      }

      @Override
      public int getLength() {
        return docIdSearchableOffset;
      }

      @Override
      public int getEndDocId() {
        return docIdSearchableOffset;
      }

      @Override
      public Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }

      @Override
      public int maxNumberOfMultiValues() {
        return maxNumberOfMultiValuesMap;
      }
    };
  }

}
