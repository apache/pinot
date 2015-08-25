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
package com.linkedin.pinot.core.segment.index.data.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.impl.FixedBitSkipListSCMVReader;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SortedSingleValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.SortedForwardIndexReader;


/**
 * Nov 15, 2014
 *
 */

public class ColumnDataSourceImpl extends DataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnDataSourceImpl.class);

  private final ColumnIndexContainer indexContainer;
  private Predicate predicate;

  private int blockNextCallCount = 0;
  boolean isPredicateEvaluated = false;

  public ColumnDataSourceImpl(ColumnIndexContainer indexContainer) {
    this.indexContainer = indexContainer;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block getNextBlock() {
    blockNextCallCount++;
    if (blockNextCallCount <= 1) {
      return nextBlock(new BlockId(0));
    }
    return null;
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    Block b = null;

    if (indexContainer.getColumnMetadata().isSingleValue()) {
      if (indexContainer.getColumnMetadata().isSorted()) {
        b =
            new SortedSingleValueBlock(blockId, (SortedForwardIndexReader) indexContainer.getForwardIndex(),
                indexContainer.getDictionary(), indexContainer.getColumnMetadata());
      } else {
        b =
            new UnSortedSingleValueBlock(blockId,
                (FixedBitCompressedSVForwardIndexReader) indexContainer.getForwardIndex(),
                indexContainer.getDictionary(), indexContainer.getColumnMetadata());
      }
    } else {
      b =
          new MultiValueBlock(blockId, (FixedBitSkipListSCMVReader) indexContainer.getForwardIndex(),
              indexContainer.getDictionary(), indexContainer.getColumnMetadata());
    }

    return b;
  }

  @Override
  public String getOperatorName() {
    return "ColumnDataSourceImpl";
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate p) {
    throw new UnsupportedOperationException("cannnot setPredicate on data source");
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return new DataSourceMetadata() {

      @Override
      public boolean isSorted() {
        return indexContainer.getColumnMetadata().isSorted();
      }

      @Override
      public boolean hasInvertedIndex() {
        if (indexContainer.getColumnMetadata().isSorted()) {
          return true;
        }
        return indexContainer.getColumnMetadata().isHasInvertedIndex() && indexContainer.getInvertedIndex() != null;
      }

      @Override
      public boolean hasDictionary() {
        return indexContainer.getColumnMetadata().hasDictionary();
      }

      @Override
      public FieldType getFieldType() {
        return indexContainer.getColumnMetadata().getFieldType();
      }

      @Override
      public DataType getDataType() {
        return indexContainer.getColumnMetadata().getDataType();
      }

      @Override
      public int cardinality() {
        return indexContainer.getColumnMetadata().getCardinality();
      }

      @Override
      public boolean isSingleValue() {
        return indexContainer.getColumnMetadata().isSingleValue();
      }
    };
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return indexContainer.getInvertedIndex();
  }

  @Override
  public Dictionary getDictionary() {
    return indexContainer.getDictionary();
  }
}
