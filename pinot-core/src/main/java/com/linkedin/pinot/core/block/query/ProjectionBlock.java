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
package com.linkedin.pinot.core.block.query;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;


/**
 * ProjectionBlock holds a column name to Block Map.
 * It provides DocIdSetBlock and DataBlock for a given column.
 *
 *
 */
public class ProjectionBlock implements Block {

  private final Map<String, Block> _blockMap = new HashMap<String, Block>();
  private final Block _docIdSetBlock;

  public ProjectionBlock(BReusableFilteredDocIdSetOperator docIdSetOperator, Map<String, DataSource> columnToDataSourceMap) {
    _docIdSetBlock = docIdSetOperator.nextBlock();
    _blockMap.put("_docIdSet", _docIdSetBlock);
    for (String column : columnToDataSourceMap.keySet()) {
      _blockMap.put(column, columnToDataSourceMap.get(column).nextBlock(new BlockId(0)));
    }
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
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

  public Block getDocIdSetBlock() {
    return _docIdSetBlock;
  }

}
