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
package org.apache.pinot.core.operator.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class StreamingSelectionOnlyOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "StreamingSelectionOnlyOperator";
  private static final String EXPLAIN_NAME = "SELECT_STREAMING";

  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final List<ExpressionContext> _expressions;
  private final BlockValSet[] _blockValSets;
  private final DataSchema _dataSchema;
  private final int _limit;

  private int _numDocsScanned = 0;

  public StreamingSelectionOnlyOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _expressions = expressions;

    int numExpressions = expressions.size();
    _blockValSets = new BlockValSet[numExpressions];
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = expressions.get(i);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      columnNames[i] = expression.toString();
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);

    _limit = queryContext.getLimit();
  }

  @Nullable
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    if (_numDocsScanned >= _limit) {
      // Already returned enough documents
      return null;
    }
    TransformBlock transformBlock = _transformOperator.nextBlock();
    if (transformBlock == null) {
      return null;
    }
    int numExpressions = _expressions.size();
    for (int i = 0; i < numExpressions; i++) {
      _blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
    }
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);
    int numDocs = transformBlock.getNumDocs();
    int numDocsToReturn = Math.min(_limit - _numDocsScanned, numDocs);
    List<Object[]> rows = new ArrayList<>(numDocsToReturn);
    for (int i = 0; i < numDocsToReturn; i++) {
      rows.add(blockValueFetcher.getRow(i));
    }
    _numDocsScanned += numDocs;
    return new IntermediateResultsBlock(_dataSchema, rows);
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
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }
}
