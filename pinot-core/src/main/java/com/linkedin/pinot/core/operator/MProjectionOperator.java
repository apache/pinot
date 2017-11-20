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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.DataBlockCache;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import java.util.HashMap;
import java.util.Map;


/**
 * MProjectionOperator will call nextBlock then return a ProjectionBlock.
 *
 *
 */
public class MProjectionOperator extends BaseOperator<ProjectionBlock> {
  private static final String OPERATOR_NAME = "MProjectionOperator";

  private final BReusableFilteredDocIdSetOperator _docIdSetOperator;
  private final Map<String, BaseOperator> _columnToDataSourceMap;
  private ProjectionBlock _currentBlock = null;
  private Map<String, Block> _blockMap;
  private DataBlockCache _dataBlockCache;
  private final DataFetcher _dataFetcher;

  public MProjectionOperator(Map<String, BaseOperator> dataSourceMap, BReusableFilteredDocIdSetOperator docIdSetOperator) {
    _docIdSetOperator = docIdSetOperator;
    _columnToDataSourceMap = dataSourceMap;
    _blockMap = new HashMap<>();
    _dataFetcher = new DataFetcher(_columnToDataSourceMap);
    _dataBlockCache = new DataBlockCache(_dataFetcher);
  }

  @Override
  protected ProjectionBlock getNextBlock() {
    DocIdSetBlock docIdSetBlock = _docIdSetOperator.nextBlock();
    if (docIdSetBlock == null) {
      _currentBlock = null;
    } else {
      _blockMap.put("_docIdSet", docIdSetBlock);
      for (String column : _columnToDataSourceMap.keySet()) {
        _blockMap.put(column, _columnToDataSourceMap.get(column).nextBlock());
      }
      _currentBlock = new ProjectionBlock(_blockMap, _dataBlockCache, docIdSetBlock);
    }
    return _currentBlock;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  // TODO: remove this method.
  public ProjectionBlock getCurrentBlock() {
    return _currentBlock;
  }

  public BaseOperator getDataSource(String column) {
    return _columnToDataSourceMap.get(column);
  }

  public int getNumProjectionColumns() {
    return _columnToDataSourceMap.size();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _docIdSetOperator.getExecutionStatistics();
  }
}
