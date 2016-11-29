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
import com.linkedin.pinot.core.operator.aggregation.DataBlockCache;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;


/**
 * Class to represent projection block for a specified column.
 */
public class ProjectionColumnBlock implements Block {
  private final DataBlockCache _dataBlockCache;
  private final String _column;

  public ProjectionColumnBlock(DataBlockCache dataBlockCache, String column) {
    _column = column;
    _dataBlockCache = dataBlockCache;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException("Operation getId() not supported on ProjectionColumnBlock.");
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException("Operation applyPredicate() not support on ProjectionColumnBlock.");
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException("Operation getBlockDocIdSet() not supported on ProjectionColumnBlock.");
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new ProjectionBlockValSet(_dataBlockCache, _column);
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Operation getBlockDocIdValueSet() not supported on ProjectionColumnBlock.");
  }

  @Override
  public BlockMetadata getMetadata() {
    return _dataBlockCache.getMetadataFor(_column);
  }
}
