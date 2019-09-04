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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformBlockDataFetcher;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


public class SelectionOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "SelectionOperator";

  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final List<SelectionSort> _sortSequence;
  private final List<TransformExpressionTree> _expressions;
  private final int _limit;
  private final int _maxRows;
  private final Collection<Serializable[]> _rows;
  private final PriorityQueue<Serializable[]> _priorityQueue;
  private final BlockValSet[] _blockValSets;
  private final DataSchema _dataSchema;

  private ExecutionStatistics _executionStatistics;

  /**
   * NOTE: The expressions in the transform operator has the sort expressions at the front. The sort sequence is the
   * deduplicated sort expressions.
   */
  public SelectionOperator(IndexSegment indexSegment, Selection selection, TransformOperator transformOperator,
      List<SelectionSort> sortSequence) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _sortSequence = sortSequence;
    _expressions = _transformOperator.getExpressions();
    _limit = selection.getSize();

    boolean selectionOnly = sortSequence.isEmpty();
    if (selectionOnly) {
      // Selection only
      _maxRows = _limit;
      _rows = new ArrayList<>(_maxRows);
      _priorityQueue = null;
    } else {
      // Selection order-by
      _maxRows = selection.getOffset() + _limit;
      _rows = null;
      _priorityQueue = new PriorityQueue<>(_maxRows, getComparator());
    }

    int numExpressions = _expressions.size();
    _blockValSets = new BlockValSet[numExpressions];
    String[] columnNames = new String[numExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      TransformExpressionTree expression = _expressions.get(i);
      columnNames[i] = expression.toString();
      TransformResultMetadata resultMetadata = _transformOperator.getResultMetadata(expression);
      columnDataTypes[i] = ColumnDataType.fromDataType(resultMetadata.getDataType(), resultMetadata.isSingleValue());
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  // TODO: Optimize the comparator by comparing dictionary ids
  private Comparator<Serializable[]> getComparator() {
    return (o1, o2) -> {
      int numSortExpressions = _sortSequence.size();
      for (int i = 0; i < numSortExpressions; i++) {
        // Only compare single-value columns
        if (!_blockValSets[i].isSingleValue()) {
          continue;
        }

        Serializable v1 = o1[i];
        Serializable v2 = o2[i];

        int ret = 0;
        SelectionSort selectionSort = _sortSequence.get(i);
        switch (_blockValSets[i].getValueType()) {
          case INT:
            if (!selectionSort.isIsAsc()) {
              ret = ((Integer) v1).compareTo((Integer) v2);
            } else {
              ret = ((Integer) v2).compareTo((Integer) v1);
            }
            break;
          case LONG:
            if (!selectionSort.isIsAsc()) {
              ret = ((Long) v1).compareTo((Long) v2);
            } else {
              ret = ((Long) v2).compareTo((Long) v1);
            }
            break;
          case FLOAT:
            if (!selectionSort.isIsAsc()) {
              ret = ((Float) v1).compareTo((Float) v2);
            } else {
              ret = ((Float) v2).compareTo((Float) v1);
            }
            break;
          case DOUBLE:
            if (!selectionSort.isIsAsc()) {
              ret = ((Double) v1).compareTo((Double) v2);
            } else {
              ret = ((Double) v2).compareTo((Double) v1);
            }
            break;
          case STRING:
            if (!selectionSort.isIsAsc()) {
              ret = ((String) v1).compareTo((String) v2);
            } else {
              ret = ((String) v2).compareTo((String) v1);
            }
            break;
          case BYTES:
            if (!selectionSort.isIsAsc()) {
              ret = ByteArray.compare((byte[]) v1, (byte[]) v2);
            } else {
              ret = ByteArray.compare((byte[]) v2, (byte[]) v1);
            }
          default:
            break;
        }
        if (ret != 0) {
          return ret;
        }
      }

      return 0;
    };
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;
    int numExpressions = _expressions.size();
    boolean selectionOnly = _sortSequence.isEmpty();

    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      for (int i = 0; i < numExpressions; i++) {
        TransformExpressionTree expression = _expressions.get(i);
        _blockValSets[i] = transformBlock.getBlockValueSet(expression);
      }
      TransformBlockDataFetcher dataFetcher = new TransformBlockDataFetcher(_blockValSets);
      int numDocs = transformBlock.getNumDocs();
      int docId = 0;
      if (selectionOnly) {
        // Selection only
        while (_rows.size() < _limit && docId < numDocs) {
          Serializable[] row = dataFetcher.getRow(docId);
          _rows.add(row);
          docId++;
        }
        numDocsScanned += docId;
        if (_rows.size() == _limit) {
          break;
        }
      } else {
        // Selection order-by
        while (docId < numDocs) {
          Serializable[] row = dataFetcher.getRow(docId);
          if (_priorityQueue.size() < _maxRows) {
            _priorityQueue.add(row);
          } else if (_priorityQueue.comparator().compare(_priorityQueue.peek(), row) < 0) {
            _priorityQueue.poll();
            _priorityQueue.offer(row);
          }
          docId++;
        }
        numDocsScanned += docId;
      }
    }

    // Create execution statistics.
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumColumnsProjected();
    long numTotalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            numTotalRawDocs);

    if (selectionOnly) {
      // Selection only
      return new IntermediateResultsBlock(_dataSchema, _rows);
    } else {
      // Selection order-by
      return new IntermediateResultsBlock(_dataSchema, _priorityQueue);
    }
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
