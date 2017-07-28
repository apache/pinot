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
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.metadata.column.ColumnMetadata;
import com.linkedin.pinot.core.operator.docvalsets.UnSortedSingleValueSet;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class UnSortedSingleValueBlock implements Block {

  final SingleColumnSingleValueReader sVReader;
  private final BlockId id;
  final ImmutableDictionaryReader dictionary;
  final ColumnMetadata columnMetadata;
  final BlockMetadata blockMetadata;
  private Predicate predicate;

  public UnSortedSingleValueBlock(BlockId id, SingleColumnSingleValueReader singleValueReader,
      ImmutableDictionaryReader dictionaryReader, ColumnMetadata columnMetadata) {
    sVReader = singleValueReader;
    this.id = id;
    dictionary = dictionaryReader;
    this.columnMetadata = columnMetadata;
    this.blockMetadata = new BlockMetadataImpl(columnMetadata, dictionaryReader);
  }

  public SingleColumnSingleValueReader getSVReader() {
    return sVReader;
  }

  public ImmutableDictionaryReader getDictionary() {
    return dictionary;
  }

  @Override
  public BlockId getId() {
    return id;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException("cannnot setPredicate on data source blocks");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException("cannnot getBlockDocIdSet on data source blocks");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new UnSortedSingleValueSet(sVReader, columnMetadata);
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
