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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.roaringbitmap.RoaringBitmap;


/**
 * Streaming only selection operator returns one block at a time not one block per-segment.
 * This is for efficient streaming of data return on a selection-only situation.
 *
 * This optimization doesn't apply to any other combine/merge required operators.
 */
public class StreamingSelectionOnlyOperator extends BaseOperator<SelectionResultsBlock> {
  private static final String EXPLAIN_NAME = "SELECT_STREAMING";

  private final IndexSegment _indexSegment;
  private final List<ExpressionContext> _expressions;
  private final BaseProjectOperator<?> _projectOperator;
  private final BlockValSet[] _blockValSets;
  private final DataSchema _dataSchema;
  private final int _limit;
  private final boolean _nullHandlingEnabled;
  private final RoaringBitmap[] _nullBitmaps;

  private int _numDocsScanned = 0;

  public StreamingSelectionOnlyOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, BaseProjectOperator<?> projectOperator) {
    _indexSegment = indexSegment;
    _expressions = expressions;
    _projectOperator = projectOperator;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();

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
    _nullBitmaps = _nullHandlingEnabled ? new RoaringBitmap[numExpressions] : null;
    _limit = queryContext.getLimit();
  }

  @Nullable
  @Override
  protected SelectionResultsBlock getNextBlock() {
    if (_numDocsScanned >= _limit) {
      // Already returned enough documents
      return null;
    }
    ValueBlock valueBlock = _projectOperator.nextBlock();
    if (valueBlock == null) {
      return null;
    }
    int numExpressions = _expressions.size();
    for (int i = 0; i < numExpressions; i++) {
      _blockValSets[i] = valueBlock.getBlockValueSet(_expressions.get(i));
    }
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(_blockValSets);
    int numDocs = valueBlock.getNumDocs();
    int numDocsToReturn = Math.min(_limit - _numDocsScanned, numDocs);
    List<Object[]> rows = new ArrayList<>(numDocsToReturn);
    if (_nullHandlingEnabled) {
      for (int i = 0; i < numExpressions; i++) {
        _nullBitmaps[i] = _blockValSets[i].getNullBitmap();
      }
      for (int docId = 0; docId < numDocsToReturn; docId++) {
        Object[] values = blockValueFetcher.getRow(docId);
        for (int colId = 0; colId < numExpressions; colId++) {
          if (_nullBitmaps[colId] != null && _nullBitmaps[colId].contains(docId)) {
            values[colId] = null;
          }
        }
        rows.add(values);
      }
    } else {
      for (int i = 0; i < numDocsToReturn; i++) {
        rows.add(blockValueFetcher.getRow(i));
      }
    }
    _numDocsScanned += numDocs;
    return new SelectionResultsBlock(_dataSchema, rows);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
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
