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
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataBlockCache;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import java.util.Map;


/**
 * ProjectionBlock holds a column name to Block Map.
 * It provides DocIdSetBlock for a given column.
 */
public class ProjectionBlock implements Block {

  private final Map<String, Block> _blockMap;
  private final DocIdSetBlock _docIdSetBlock;
  private final DataBlockCache _dataBlockCache;

  public ProjectionBlock(Map<String, Block> blockMap, DataBlockCache dataBlockCache, DocIdSetBlock docIdSetBlock) {
    _blockMap = blockMap;
    _docIdSetBlock = docIdSetBlock;
    _dataBlockCache = dataBlockCache;
    _dataBlockCache.initNewBlock(docIdSetBlock.getDocIdSet(), 0, docIdSetBlock.getSearchableLength());
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  public Block getBlock(String column) {
    return _blockMap.get(column);
  }

  public BlockValSet getBlockValueSet(String column) {
    return new ProjectionBlockValSet(_dataBlockCache, column);
  }

  public BlockMetadata getMetadata(String column) {
    return _blockMap.get(column).getMetadata();
  }

  public DocIdSetBlock getDocIdSetBlock() {
    return _docIdSetBlock;
  }

  public int getNumDocs() {
    return _docIdSetBlock.getSearchableLength();
  }
}
