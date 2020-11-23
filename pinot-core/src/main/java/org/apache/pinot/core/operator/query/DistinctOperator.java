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
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.distinct.DistinctTable;


/**
 * Operator for distinct queries on a single segment.
 */
public class DistinctOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "DistinctOperator";

  private final IndexSegment _indexSegment;
  private final DistinctAggregationFunction _distinctAggregationFunction;
  private final TransformOperator _transformOperator;
  private final DistinctExecutor _distinctExecutor;

  private int _numDocsScanned = 0;

  public DistinctOperator(IndexSegment indexSegment, DistinctAggregationFunction distinctAggregationFunction,
      TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _distinctAggregationFunction = distinctAggregationFunction;
    _transformOperator = transformOperator;
    _distinctExecutor = DistinctExecutorFactory.getDistinctExecutor(distinctAggregationFunction, transformOperator);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      if (_distinctExecutor.process(transformBlock)) {
        break;
      }
    }
    DistinctTable distinctTable = _distinctExecutor.getResult();
    // TODO: Use a separate way to represent DISTINCT instead of aggregation.
    return new IntermediateResultsBlock(new AggregationFunction[]{_distinctAggregationFunction},
        Collections.singletonList(distinctTable), false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }
}
