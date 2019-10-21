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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


public class SelectionOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "SelectionOrderByOperator";

  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final List<SelectionSort> _sortSequence;
  private final List<TransformExpressionTree> _expressions;
  private final TransformResultMetadata[] _expressionMetadata;
  private final DataSchema _dataSchema;
  private final int _numRowsToKeep;
  private final PriorityQueue<Serializable[]> _rows;

  private ExecutionStatistics _executionStatistics;

  public SelectionOrderByOperator(IndexSegment indexSegment, Selection selection, TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _sortSequence = selection.getSelectionSortSequence();
    _expressions =
        SelectionOperatorUtils.extractExpressions(selection.getSelectionColumns(), indexSegment, _sortSequence);

    int numExpressions = _expressions.size();
    _expressionMetadata = new TransformResultMetadata[numExpressions];
    String[] columnNames = new String[numExpressions];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      TransformExpressionTree expression = _expressions.get(i);
      TransformResultMetadata expressionMetadata = _transformOperator.getResultMetadata(expression);
      _expressionMetadata[i] = expressionMetadata;
      columnNames[i] = expression.toString();
      columnDataTypes[i] =
          DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(), expressionMetadata.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);

    _numRowsToKeep = selection.getOffset() + selection.getSize();
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getComparator());
  }

  private Comparator<Serializable[]> getComparator() {
    // Compare all single-value columns
    int numOrderByExpressions = _sortSequence.size();
    List<Integer> valueIndexList = new ArrayList<>(numOrderByExpressions);
    for (int i = 0; i < numOrderByExpressions; i++) {
      if (_expressionMetadata[i].isSingleValue()) {
        valueIndexList.add(i);
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    Comparator[] valueComparators = new Comparator[numValuesToCompare];
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      switch (_expressionMetadata[valueIndex].getDataType()) {
        case INT:
          valueComparators[i] = (Comparator<Integer>) Integer::compare;
          break;
        case LONG:
          valueComparators[i] = (Comparator<Long>) Long::compare;
          break;
        case FLOAT:
          valueComparators[i] = (Comparator<Float>) Float::compare;
          break;
        case DOUBLE:
          valueComparators[i] = (Comparator<Double>) Double::compare;
          break;
        case STRING:
          valueComparators[i] = Comparator.naturalOrder();
          break;
        case BYTES:
          valueComparators[i] = (Comparator<byte[]>) ByteArray::compare;
          break;
        default:
          throw new IllegalStateException();
      }
      if (_sortSequence.get(valueIndex).isIsAsc()) {
        valueComparators[i] = valueComparators[i].reversed();
      }
    }

    return new SelectionOperatorUtils.RowComparator(valueIndices, valueComparators);
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      int numExpressions = _expressions.size();
      BlockValSet[] blockValSets = new BlockValSet[numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        TransformExpressionTree expression = _expressions.get(i);
        blockValSets[i] = transformBlock.getBlockValueSet(expression);
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);

      int numDocsFetched = transformBlock.getNumDocs();
      numDocsScanned += numDocsFetched;
      for (int i = 0; i < numDocsFetched; i++) {
        SelectionOperatorUtils.addToPriorityQueue(blockValueFetcher.getRow(i), _rows, _numRowsToKeep);
      }
    }

    // Create execution statistics.
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumColumnsProjected();
    long numTotalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            numTotalRawDocs);

    return new IntermediateResultsBlock(_dataSchema, _rows);
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
