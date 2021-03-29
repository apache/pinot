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
package org.apache.pinot.core.operator;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class ProjectionOperator extends BaseOperator<ProjectionBlock> {
  private static final String OPERATOR_NAME = "ProjectionOperator";

  private final Map<String, DataSource> _dataSourceMap;
  private final BaseOperator<DocIdSetBlock> _docIdSetOperator;
  private final DataBlockCache _dataBlockCache;

  public ProjectionOperator(Map<String, DataSource> dataSourceMap,
      @Nullable BaseOperator<DocIdSetBlock> docIdSetOperator) {
    _dataSourceMap = dataSourceMap;
    _docIdSetOperator = docIdSetOperator;
    _dataBlockCache = new DataBlockCache(new DataFetcher(dataSourceMap));
  }

  /**
   * Returns the map from column to data source.
   *
   * @return Map from column to data source
   */
  public Map<String, DataSource> getDataSourceMap() {
    return _dataSourceMap;
  }

  @Override
  protected ProjectionBlock getNextBlock() {
    // NOTE: Should not be called when _docIdSetOperator is null.
    assert _docIdSetOperator != null;
    DocIdSetBlock docIdSetBlock = _docIdSetOperator.nextBlock();
    if (docIdSetBlock == null) {
      return null;
    } else {
      _dataBlockCache.initNewBlock(docIdSetBlock.getDocIdSet(), docIdSetBlock.getSearchableLength());
      return new ProjectionBlock(_dataSourceMap, _dataBlockCache);
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _docIdSetOperator != null ? _docIdSetOperator.getExecutionStatistics() : new ExecutionStatistics(0, 0, 0, 0);
  }
}
