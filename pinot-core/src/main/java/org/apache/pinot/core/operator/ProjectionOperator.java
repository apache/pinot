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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.trace.Tracing;


public class ProjectionOperator extends BaseProjectOperator<ProjectionBlock> {
  private static final String EXPLAIN_NAME = "PROJECT";

  private final Map<String, DataSource> _dataSourceMap;
  private final BaseOperator<DocIdSetBlock> _docIdSetOperator;
  private final DataBlockCache _dataBlockCache;
  private final Map<String, ColumnContext> _columnContextMap;

  public ProjectionOperator(Map<String, DataSource> dataSourceMap,
      @Nullable BaseOperator<DocIdSetBlock> docIdSetOperator) {
    _dataSourceMap = dataSourceMap;
    _docIdSetOperator = docIdSetOperator;
    _dataBlockCache = new DataBlockCache(new DataFetcher(dataSourceMap));
    _columnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(dataSourceMap.size()));
    dataSourceMap.forEach(
        (column, dataSource) -> _columnContextMap.put(column, ColumnContext.fromDataSource(dataSource)));
  }

  @Override
  public Map<String, ColumnContext> getSourceColumnContextMap() {
    return _columnContextMap;
  }

  @Override
  public ColumnContext getResultColumnContext(ExpressionContext expression) {
    assert expression.getType() == ExpressionContext.Type.IDENTIFIER;
    return _columnContextMap.get(expression.getIdentifier());
  }

  @Override
  protected ProjectionBlock getNextBlock() {
    // NOTE: Should not be called when _docIdSetOperator is null.
    assert _docIdSetOperator != null;
    DocIdSetBlock docIdSetBlock = _docIdSetOperator.nextBlock();
    if (docIdSetBlock == null) {
      return null;
    } else {
      Tracing.activeRecording().setNumChildren(_dataSourceMap.size());
      _dataBlockCache.initNewBlock(docIdSetBlock.getDocIds(), docIdSetBlock.getLength());
      return new ProjectionBlock(_dataSourceMap, _dataBlockCache);
    }
  }

  @Override
  public List<Operator<DocIdSetBlock>> getChildOperators() {
    return Collections.singletonList(_docIdSetOperator);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append('(');
    // SQL statements such as SELECT 'literal' FROM myTable don't have any projection columns.
    if (!_dataSourceMap.keySet().isEmpty()) {
      int count = 0;
      for (String col : _dataSourceMap.keySet()) {
        if (count == _dataSourceMap.keySet().size() - 1) {
          stringBuilder.append(col);
        } else {
          stringBuilder.append(col).append(", ");
        }
        count++;
      }
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _docIdSetOperator != null ? _docIdSetOperator.getExecutionStatistics() : new ExecutionStatistics(0, 0, 0, 0);
  }
}
