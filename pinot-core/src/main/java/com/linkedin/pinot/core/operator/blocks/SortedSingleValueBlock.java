/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.io.reader.impl.SortedForwardIndexReader;
import com.linkedin.pinot.core.metadata.column.ColumnMetadata;
import com.linkedin.pinot.core.operator.docvalsets.SortedSingleValueSet;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;

/**
 * Nov 15, 2014
 */

public class SortedSingleValueBlock implements Block {

  final SortedForwardIndexReader sVReader;
  private final BlockId id;
  private final BlockMetadata blockMetadata;

  public SortedSingleValueBlock(BlockId id, SortedForwardIndexReader singleValueReader,
      ImmutableDictionaryReader dictionaryReader, ColumnMetadata columnMetadata) {
    sVReader = singleValueReader;
    this.id = id;
    this.blockMetadata = new BlockMetadataImpl(columnMetadata, dictionaryReader);
  }

  @Override
  public BlockId getId() {
    return id;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException("cannnot set predicate on blocks");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException("cannnot getBlockDocIdSet on data source blocks");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new SortedSingleValueSet(sVReader);
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    return blockMetadata;
  }
}
