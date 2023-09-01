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
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.roaringbitmap.RoaringBitmap;


public class SelectionOnlyOperator extends BaseOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT";

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final boolean _nullHandlingEnabled;
  private final BaseProjectOperator<?> _projectOperator;
  private final List<ExpressionContext> _expressions;
  private final BlockValSet[] _blockValSets;
  private final DataSchema _dataSchema;
  private final int _numRowsToKeep;
  private final ArrayList<Object[]> _rows;
  private final RoaringBitmap[] _nullBitmaps;

  private int _numDocsScanned = 0;

  public SelectionOnlyOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _projectOperator = projectOperator;
    _expressions = expressions;

    int numExpressions = expressions.size();
    _blockValSets = new BlockValSet[numExpressions];
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      ExpressionContext expression = expressions.get(i);
      columnNames[i] = expression.toString();
      ColumnContext columnContext = projectOperator.getResultColumnContext(expression);
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(columnContext.getDataType(), columnContext.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);

    _numRowsToKeep = queryContext.getLimit();
    _rows = new ArrayList<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    _nullBitmaps = _nullHandlingEnabled ? new RoaringBitmap[numExpressions] : null;
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
  protected SelectionResultsBlock getNextBlock() {
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      int numExpressions = _expressions.size();
      for (int i = 0; i < numExpressions; i++) {
        _blockValSets[i] = valueBlock.getBlockValueSet(_expressions.get(i));
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);

      int numDocsToAdd = Math.min(_numRowsToKeep - _rows.size(), valueBlock.getNumDocs());
      _rows.ensureCapacity(_rows.size() + numDocsToAdd);
      _numDocsScanned += numDocsToAdd;
      if (_nullHandlingEnabled) {
        for (int i = 0; i < numExpressions; i++) {
          _nullBitmaps[i] = _blockValSets[i].getNullBitmap();
        }
        for (int docId = 0; docId < numDocsToAdd; docId++) {
          Object[] values = blockValueFetcher.getRow(docId);
          for (int colId = 0; colId < numExpressions; colId++) {
            if (_nullBitmaps[colId] != null && _nullBitmaps[colId].contains(docId)) {
              values[colId] = null;
            }
          }
          _rows.add(values);
        }
      } else {
        for (int i = 0; i < numDocsToAdd; i++) {
          _rows.add(blockValueFetcher.getRow(i));
        }
      }
      if (_rows.size() == _numRowsToKeep) {
        break;
      }
    }

    return new SelectionResultsBlock(_dataSchema, _rows, _queryContext);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }
}
