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

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlockWithBitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SingleValueBlockWithBitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 15, 2014
 *
 */

public class ColumnDataSourceImpl implements DataSource {
  private static final Logger logger = Logger.getLogger(ColumnDataSourceImpl.class);

  private final ImmutableDictionaryReader dictionary;
  private final DataFileReader reader;
  private final InvertedIndexReader invertedIndex;
  private final ColumnMetadata columnMetadata;
  private Predicate predicate;
  private int blockNextCallCount = 0;
  boolean isPredicateEvaluated = false;

  public ColumnDataSourceImpl(ImmutableDictionaryReader dictionary, DataFileReader reader,
      InvertedIndexReader invertedIndex, ColumnMetadata columnMetadata) {
    this.dictionary = dictionary;
    this.reader = reader;
    this.invertedIndex = invertedIndex;
    this.columnMetadata = columnMetadata;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    blockNextCallCount++;
    if (blockNextCallCount <= 1) {
      return nextBlock(new BlockId(0));
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId blockId) {
    Block b;
    if (columnMetadata.isSingleValue()) {
      b =
          new SingleValueBlockWithBitmapInvertedIndex(blockId, (FixedBitCompressedSVForwardIndexReader) reader,
              invertedIndex, dictionary, columnMetadata);
    } else {
      b =
          new MultiValueBlockWithBitmapInvertedIndex(blockId, (FixedBitCompressedMVForwardIndexReader) reader,
              invertedIndex, dictionary, columnMetadata);
    }

    if (predicate != null) {
      b.applyPredicate(predicate);
    }
    return b;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate p) {
    predicate = p;
    return true;
  }
}
