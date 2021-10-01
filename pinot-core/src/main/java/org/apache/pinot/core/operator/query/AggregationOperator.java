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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.startree.executor.StarTreeAggregationExecutor;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation only query on a single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationOperator";
  private static final String EXPLAIN_NAME = "AGGREGATE";

  private final AggregationFunction[] _aggregationFunctions;
  private final TransformOperator _transformOperator;
  private final long _numTotalDocs;
  private final boolean _useStarTree;

  private int _numDocsScanned = 0;

  public AggregationOperator(AggregationFunction[] aggregationFunctions, TransformOperator transformOperator,
      long numTotalDocs, boolean useStarTree) {
    _aggregationFunctions = aggregationFunctions;
    _transformOperator = transformOperator;
    _numTotalDocs = numTotalDocs;
    _useStarTree = useStarTree;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    // Perform aggregation on all the transform blocks
    AggregationExecutor aggregationExecutor;
    if (_useStarTree) {
      aggregationExecutor = new StarTreeAggregationExecutor(_aggregationFunctions);
    } else {
      aggregationExecutor = new DefaultAggregationExecutor(_aggregationFunctions);
    }
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      aggregationExecutor.aggregate(transformBlock);
    }

    // Build intermediate result block based on aggregation result from the executor
    return new IntermediateResultsBlock(_aggregationFunctions, aggregationExecutor.getResult(), false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Arrays.asList(_transformOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getExplainPlanName()).append("(aggregations:");
    int count = 0;
    for (AggregationFunction func : _aggregationFunctions) {
      if (count == _aggregationFunctions.length - 1) {
        stringBuilder.append(func.toExplainString());
      } else {
        stringBuilder.append(func.toExplainString()).append(", ");
      }
      count++;
    }
    return stringBuilder.append(')').toString();
  }
}
