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

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;


/**
 * The <code>AggregationGroupByOrderByOperator</code> class provides the operator for aggregation group-by query on a
 * single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByOrderByOperator";

  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final TransformOperator _transformOperator;
  private final long _numTotalDocs;
  private final boolean _useStarTree;
  private final DataSchema _dataSchema;

  private int _numDocsScanned = 0;

  public AggregationGroupByOrderByOperator(AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, int maxInitialResultHolderCapacity, int numGroupsLimit,
      TransformOperator transformOperator, long numTotalDocs, boolean useStarTree) {
    _aggregationFunctions = aggregationFunctions;
    _groupByExpressions = groupByExpressions;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _transformOperator = transformOperator;
    _numTotalDocs = numTotalDocs;
    _useStarTree = useStarTree;

    // NOTE: The indexedTable expects that the the data schema will have group by columns before aggregation columns
    int numGroupByExpressions = groupByExpressions.length;
    int numAggregationFunctions = aggregationFunctions.length;
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      columnNames[i] = groupByExpression.toString();
      columnDataTypes[i] = DataSchema.ColumnDataType
          .fromDataTypeSV(_transformOperator.getResultMetadata(groupByExpression).getDataType());
    }

    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = aggregationFunctions[i];
      int index = numGroupByExpressions + i;
      columnNames[index] = aggregationFunction.getResultColumnName();
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    if (_useStarTree) {
      groupByExecutor =
          new StarTreeGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, _transformOperator);
    } else {
      groupByExecutor =
          new DefaultGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, _transformOperator);
    }
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      groupByExecutor.process(transformBlock);
    }

    // Build intermediate result block based on aggregation group-by result from the executor
    return new IntermediateResultsBlock(_aggregationFunctions, groupByExecutor.getResult(), _dataSchema);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }
}
