/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.blocks;

import java.util.Map;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;


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

  public BlockValSet getBlockValueSet(String column) {
    BlockMetadata blockMetadata = _blockMap.get(column).getMetadata();
    return new ProjectionBlockValSet(_dataBlockCache, column, blockMetadata.getDataType(),
        blockMetadata.isSingleValue());
  }

  public DocIdSetBlock getDocIdSetBlock() {
    return _docIdSetBlock;
  }

  public int getNumDocs() {
    return _docIdSetBlock.getSearchableLength();
  }
}
