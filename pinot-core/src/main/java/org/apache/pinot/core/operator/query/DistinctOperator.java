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
package org.apache.pinot.core.operator.query;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Operator for distinct queries on a single segment.
 */
public class DistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT";

  private final IndexSegment _indexSegment;
  private final DistinctAggregationFunction _distinctAggregationFunction;
  private final TransformOperator _transformOperator;
  private final DistinctExecutor _distinctExecutor;

  private int _numDocsScanned = 0;

  public DistinctOperator(IndexSegment indexSegment, DistinctAggregationFunction distinctAggregationFunction,
      TransformOperator transformOperator, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _distinctAggregationFunction = distinctAggregationFunction;
    _transformOperator = transformOperator;
    _distinctExecutor = DistinctExecutorFactory.getDistinctExecutor(distinctAggregationFunction, transformOperator,
        queryContext.isNullHandlingEnabled());
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      if (_distinctExecutor.process(transformBlock)) {
        break;
      }
    }
    return new DistinctResultsBlock(_distinctAggregationFunction, _distinctExecutor.getResult());
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_transformOperator);
  }

  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }

  @Override
  public String toExplainString() {
    String[] keys = _distinctAggregationFunction.getColumns();
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(keyColumns:");
    if (keys.length > 0) {
      stringBuilder.append(keys[0]);
      for (int i = 1; i < keys.length; i++) {
        stringBuilder.append(", ").append(keys[i]);
      }
    }
    return stringBuilder.append(')').toString();
  }
}
