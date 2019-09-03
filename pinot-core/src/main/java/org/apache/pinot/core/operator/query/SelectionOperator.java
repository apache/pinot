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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
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
import org.apache.pinot.core.segment.index.readers.Dictionary;


/**
 * This SelectionOnlyOperator will take care of applying a selection query to one IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 */
public class SelectionOperator extends BaseOperator<IntermediateResultsBlock> {

  private static final String OPERATOR_NAME = "SelectionOnlyOperator";

  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final DataSchema _dataSchema;
  private final BlockValSet[] _blockValSets;
  private final int _limit;
  private Selection _selection;
  private final int _offset;
  private final int _maxRows;
  private final List<TransformExpressionTree> _expressions;
  private final List<TransformExpressionTree> _selectExpressions;
  private final List<TransformExpressionTree> _orderByExpressions;
  private final List<Integer> _orderByIndices;
  private final TransformResultMetadata[] _expressionResultMetadata;
  private final Dictionary[] _dictionaries;
  private Collection<Serializable[]> _rowEvents;
  private PriorityQueue<Serializable[]> _priorityQueue;

  private ExecutionStatistics _executionStatistics;

  public SelectionOperator(IndexSegment indexSegment, Selection selection, TransformOperator transformOperator) {
    _indexSegment = indexSegment;
    _offset = selection.getOffset();
    _limit = selection.getSize();
    _selection = selection;
    _maxRows = _offset + _limit;
    _transformOperator = transformOperator;
    _expressions = _transformOperator.getExpressions();

    List<String> selectColumns = selection.getSelectionColumns();
    if (selectColumns.size() == 1 && selectColumns.get(0).equals("*")) {
      selectColumns = new LinkedList<>(indexSegment.getPhysicalColumnNames());
    }

    List<String> orderByColumns = new ArrayList<>();
    if (selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
        String expression = selectionSort.getColumn();
        orderByColumns.add(expression);
      }
    }
    _selectExpressions = new ArrayList<>();
    _orderByExpressions = new ArrayList<>();
    _orderByIndices = new ArrayList<>();
    for (int i = 0; i < _expressions.size(); i++) {
      TransformExpressionTree expression = _expressions.get(i);
      if (selectColumns.contains(expression.toString())) {
        _selectExpressions.add(expression);
      }
    }

    for (String orderByColumn : orderByColumns) {
      for (int i = 0; i < _expressions.size(); i++) {
        TransformExpressionTree expression = _expressions.get(i);
        if (orderByColumn.equalsIgnoreCase(expression.toString())) {
          _orderByExpressions.add(expression);
          _orderByIndices.add(i);
        }
      }
    }

    _blockValSets = new BlockValSet[_expressions.size()];
    _expressionResultMetadata = new TransformResultMetadata[_expressions.size()];
    _dictionaries = new Dictionary[_expressions.size()];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_expressions.size()];
    String[] columnNames = new String[_expressions.size()];
    for (int i = 0; i < _expressions.size(); i++) {
      TransformExpressionTree expression = _expressions.get(i);
      _expressionResultMetadata[i] = _transformOperator.getResultMetadata(expression);
      columnDataTypes[i] = DataSchema.ColumnDataType
          .fromDataType(_expressionResultMetadata[i].getDataType(), _expressionResultMetadata[i].isSingleValue());
      columnNames[i] = expression.toString();
      if (_expressionResultMetadata[i].hasDictionary()) {
        _dictionaries[i] = _transformOperator.getDictionary(expression);
      }
    }
    _dataSchema = new DataSchema(columnNames, columnDataTypes);
    if (_orderByExpressions.isEmpty()) {
      _rowEvents = new ArrayList<>();
    } else {
      Comparator<Serializable[]> comparator = getStrictComparator();
      _priorityQueue = new PriorityQueue<>(_maxRows, comparator);
    }
  }

  private Comparator<Serializable[]> getStrictComparator() {
    return new Comparator<Serializable[]>() {
      @Override
      public int compare(Serializable[] o1, Serializable[] o2) {
        List<SelectionSort> sortSequence = _selection.getSelectionSortSequence();
        int numSortColumns = sortSequence.size();
        for (int i = 0; i < numSortColumns; i++) {
          int ret = 0;
          SelectionSort selectionSort = sortSequence.get(i);
          int index = _orderByIndices.get(i);
          // Only compare single-value columns.
          if (!_expressionResultMetadata[index].isSingleValue()) {
            continue;
          }

          Serializable v1 = o1[index];
          Serializable v2 = o2[index];

          DataType dataType = _expressionResultMetadata[index].getDataType();
          switch (dataType) {
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
            case BOOLEAN:
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
      }
    };
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    TransformBlock transformBlock;
    boolean selectionOnly = _orderByExpressions.isEmpty();
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      for (int i = 0; i < _expressions.size(); i++) {
        TransformExpressionTree expression = _expressions.get(i);
        _blockValSets[i] = transformBlock.getBlockValueSet(expression);
      }
      TransformBlockDataFetcher dataFetcher;
      dataFetcher = new TransformBlockDataFetcher(_blockValSets, _dictionaries, _expressionResultMetadata);
      if (selectionOnly) {
        int docId = 0;
        while (_rowEvents.size() < _limit && docId < transformBlock.getNumDocs()) {
          Serializable[] row = dataFetcher.getRow(docId);
          _rowEvents.add(row);
          docId = docId + 1;
        }
        if (_rowEvents.size() >= _limit) {
          numDocsScanned += docId;
          break;
        }
      } else {
        //Selection + orderby
        int docId = 0;
        while (docId < transformBlock.getNumDocs()) {
          Serializable[] row = dataFetcher.getRow(docId);
          if (_priorityQueue.size() < _maxRows) {
            _priorityQueue.add(row);
          } else if (_priorityQueue.comparator().compare(_priorityQueue.peek(), row) < 0) {
            _priorityQueue.poll();
            _priorityQueue.offer(row);
          }
          docId = docId + 1;
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
      return new IntermediateResultsBlock(_dataSchema, _rowEvents);
    } else {
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



