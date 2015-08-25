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
package com.linkedin.pinot.core.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;


/**
 * MProjectionOperator will call nextBlock then return a ProjectionBlock.
 *
 *
 */
public class MProjectionOperator extends BaseOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MProjectionOperator.class);

  private final BReusableFilteredDocIdSetOperator _docIdSetOperator;
  private final Map<String, DataSource> _columnToDataSourceMap;
  private ProjectionBlock _currentBlock = null;

  public MProjectionOperator(Map<String, DataSource> dataSourceMap, BReusableFilteredDocIdSetOperator docIdSetOperator) {
    _docIdSetOperator = docIdSetOperator;
    _columnToDataSourceMap = dataSourceMap;
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
  public Block getNextBlock() {
    _currentBlock = new ProjectionBlock(_docIdSetOperator, _columnToDataSourceMap);
    if (_currentBlock.getDocIdSetBlock() == null) {
      return null;
    }
    return _currentBlock;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not supported in MProjectionOperator!");
  }

  @Override
  public String getOperatorName() {
    return "MProjectionOperator";
  }

  public ProjectionBlock getCurrentBlock() {
    return _currentBlock;
  }

  //  public DictionaryReader getDictionary(String column) {
  //    if (_columnToDataSourceMap.get(column) instanceof ColumnDataSourceImpl) {
  //      return ((ColumnDataSourceImpl) _columnToDataSourceMap.get(column)).getDictionary();
  //    } else {
  //      throw new UnsupportedOperationException("Not support getDictionary for DataSource: "
  //          + _columnToDataSourceMap.get(column));
  //    }
  //  }

  public DataSource getDataSource(String column) {
    return _columnToDataSourceMap.get(column);
  }

}
