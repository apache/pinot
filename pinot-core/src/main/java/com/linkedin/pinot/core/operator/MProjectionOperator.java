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
package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;


/**
 * MProjectionOperator will call nextBlock then return a ProjectionBlock.
 *
 *
 */
public class MProjectionOperator extends BaseOperator {

  private final BReusableFilteredDocIdSetOperator _docIdSetOperator;
  private final Map<String, BaseOperator> _columnToDataSourceMap;
  private ProjectionBlock _currentBlock = null;
  private Map<String, Block> _blockMap;

  public MProjectionOperator(Map<String, BaseOperator> dataSourceMap, BReusableFilteredDocIdSetOperator docIdSetOperator) {
    _docIdSetOperator = docIdSetOperator;
    _columnToDataSourceMap = dataSourceMap;
    _blockMap = new HashMap<>();
  }

  @Override
  public boolean open() {
    for (final String column : _columnToDataSourceMap.keySet()) {
      _columnToDataSourceMap.get(column).open();
    }
    _docIdSetOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    for (final String column : _columnToDataSourceMap.keySet()) {
      _columnToDataSourceMap.get(column).close();
    }
    _docIdSetOperator.close();
    return true;
  }

  @Override
  public ProjectionBlock getNextBlock() {
    DocIdSetBlock docIdSetBlock = (DocIdSetBlock) _docIdSetOperator.nextBlock();
    if (docIdSetBlock == null) {
      _currentBlock = null;
    } else {
      _blockMap.put("_docIdSet", docIdSetBlock);
      for (String column : _columnToDataSourceMap.keySet()) {
        _blockMap.put(column, _columnToDataSourceMap.get(column).nextBlock(new BlockId(0)));
      }
      _currentBlock = new ProjectionBlock(_blockMap, docIdSetBlock);
    }
    return _currentBlock;
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    throw new UnsupportedOperationException("Not supported in MProjectionOperator!");
  }

  // TODO: remove this method.
  public ProjectionBlock getCurrentBlock() {
    return _currentBlock;
  }

  public BaseOperator getDataSource(String column) {
    return _columnToDataSourceMap.get(column);
  }

  public Map<String, BaseOperator> getDataSourceMap() {
    return _columnToDataSourceMap;
  }

  public int getNumProjectionColumns() {
    return _columnToDataSourceMap.size();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _docIdSetOperator.getExecutionStatistics();
  }
}
