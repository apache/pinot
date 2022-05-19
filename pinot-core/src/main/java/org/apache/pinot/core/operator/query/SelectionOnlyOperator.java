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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class SelectionOnlyOperator extends BaseOperator<IntermediateResultsBlock> {

  private static final String EXPLAIN_NAME = "SELECT";

  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final List<ExpressionContext> _expressions;
  private final BlockValSet[] _blockValSets;
  private final DataSchema _dataSchema;
  private final int _numRowsToKeep;
  private final List<Object[]> _rows;
  private final ImmutableRoaringBitmap[] _nullBitmaps;

  private int _numDocsScanned = 0;

  public SelectionOnlyOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _expressions = expressions;

    int numExpressions = _expressions.size();
    _blockValSets = new BlockValSet[numExpressions];
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    FieldSpec[] columnFieldSpecs = new FieldSpec[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = _expressions.get(i);
      TransformResultMetadata expressionMetadata = _transformOperator.getResultMetadata(expression);
      columnNames[i] = expression.toString();
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
      columnFieldSpecs[i] = _transformOperator.getFieldSpec(expression);
      if (columnFieldSpecs[i] == null) {
        columnFieldSpecs[i] = new MetricFieldSpec(columnNames[i], columnDataTypes[i].toDataType().getStoredType(),
            null);
      }
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes, columnFieldSpecs);

    _numRowsToKeep = queryContext.getLimit();
    _rows = new ArrayList<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    _nullBitmaps = new ImmutableRoaringBitmap[numExpressions];
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(selectList:");
    if (!_expressions.isEmpty()) {
      stringBuilder.append(_expressions.get(0));
      for (int i = 1; i < _expressions.size(); i++) {
        stringBuilder.append(", ").append(_expressions.get(i));
      }
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    TransformBlock transformBlock;
    // todo: hide null handling code behind a flag (nullHandlingEnabledInSelect).
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      int numDocsToAdd = Math.min(_numRowsToKeep - _rows.size(), transformBlock.getNumDocs());

      int numExpressions = _expressions.size();
      for (int i = 0; i < numExpressions; i++) {
        // todo: how to concatenate NullBitmap from diff. blocks?
        _blockValSets[i] = transformBlock.getBlockValueSet(_expressions.get(i));
        _nullBitmaps[i] = _blockValSets[i].getNullBitmap();
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);

      _numDocsScanned += numDocsToAdd;
      for (int docId = 0; docId < numDocsToAdd; docId++) {
        Object[] values = blockValueFetcher.getRow(docId);
        for (int colId = 0; colId < numExpressions; colId++) {
          if (_nullBitmaps[colId].contains(docId)) {
            values[colId] = null;
          }
        }
        _rows.add(values);
      }
      if (_rows.size() == _numRowsToKeep) {
        break;
      }
    }

    return new IntermediateResultsBlock(_dataSchema, _rows);
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
}
