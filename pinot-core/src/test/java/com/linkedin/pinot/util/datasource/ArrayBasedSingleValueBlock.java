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
package com.linkedin.pinot.util.datasource;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class ArrayBasedSingleValueBlock implements Block {

  private final int[] dictionary;
  final int[] values;

  public ArrayBasedSingleValueBlock(final int[] dicIds, final int[] values) {
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
    return new ArrayBasedSingleValueSet(values);
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
          return new IntArrayBackedImmutableDictionary(null, dictionary.length, 1, dictionary);
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
