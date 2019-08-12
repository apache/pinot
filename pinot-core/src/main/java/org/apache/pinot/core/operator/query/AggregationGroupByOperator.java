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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation group-by query on a single segment.
 */
public class AggregationGroupByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByOperator";

  private final DataSchema _dataSchema;
  private final AggregationFunctionContext[] _functionContexts;
  private final GroupBy _groupBy;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final TransformOperator _transformOperator;
  private final long _numTotalRawDocs;
  private final boolean _useStarTree;

  private ExecutionStatistics _executionStatistics;

  public AggregationGroupByOperator(@Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit, @Nonnull TransformOperator transformOperator,
      long numTotalRawDocs, boolean useStarTree) {
    _functionContexts = functionContexts;
    _groupBy = groupBy;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _transformOperator = transformOperator;
    _numTotalRawDocs = numTotalRawDocs;
    _useStarTree = useStarTree;

    int numColumns = groupBy.getExpressionsSize() + functionContexts.length;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    Map<String, DataSchema.ColumnDataType> columnToDataType = new HashMap<>(numColumns);
    for (TransformExpressionTree transformExpression : _transformOperator.getExpressions()) {
      columnToDataType.put(transformExpression.toString(), DataSchema.ColumnDataType.fromDataType(
          _transformOperator.getResultMetadata(transformExpression).getDataType(), true));
    }

    int index = 0;
    for (String groupByColumn : groupBy.getExpressions()) {
      columnNames[index] = groupByColumn;
      columnDataTypes[index] = columnToDataType.get(groupByColumn);
      index++;
    }

    for (AggregationFunctionContext functionContext : functionContexts) {
      columnNames[index] = functionContext.getAggregationFunction().getType().toString().toLowerCase() + "("
          + functionContext.getColumn() + ")";
      columnDataTypes[index] = functionContext.getAggregationFunction().getIntermediateResultColumnType();
      index++;
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    if (_useStarTree) {
      groupByExecutor =
          new StarTreeGroupByExecutor(_functionContexts, _groupBy, _maxInitialResultHolderCapacity, _numGroupsLimit,
              _transformOperator);
    } else {
      groupByExecutor =
          new DefaultGroupByExecutor(_functionContexts, _groupBy, _maxInitialResultHolderCapacity, _numGroupsLimit,
              _transformOperator);
    }
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      numDocsScanned += transformBlock.getNumDocs();
      groupByExecutor.process(transformBlock);
    }
    AggregationGroupByResult groupByResult = groupByExecutor.getResult();

    // Gather execution statistics
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumColumnsProjected();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation group-by result from the executor
    return new IntermediateResultsBlock(_functionContexts, groupByResult, _dataSchema);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
