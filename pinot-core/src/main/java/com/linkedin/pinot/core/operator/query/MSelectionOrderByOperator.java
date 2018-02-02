/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import java.util.HashSet;
import java.util.Set;


/**
 * This MSelectionOperator will take care of applying a selection query to one IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 *
 */
public class MSelectionOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "MSelectionOrderByOperator";

  private final IndexSegment _indexSegment;
  private final MProjectionOperator _projectionOperator;
  private final Selection _selection;
  private final SelectionOperatorService _selectionOperatorService;
  private final DataSchema _dataSchema;
  private final Block[] _blocks;
  private final Set<String> _selectionColumns = new HashSet<>();
  private ExecutionStatistics _executionStatistics;

  public MSelectionOrderByOperator(IndexSegment indexSegment, Selection selection, Operator projectionOperator) {
    _indexSegment = indexSegment;
    _selection = selection;
    _projectionOperator = (MProjectionOperator) projectionOperator;

    initColumnarDataSourcePlanNodeMap(indexSegment);
    _selectionOperatorService = new SelectionOperatorService(_selection, indexSegment);
    _dataSchema = _selectionOperatorService.getDataSchema();
    _blocks = new Block[_selectionColumns.size()];
  }

  private void initColumnarDataSourcePlanNodeMap(IndexSegment indexSegment) {
    _selectionColumns.addAll(_selection.getSelectionColumns());
    if ((_selectionColumns.size() == 1) && ((_selectionColumns.toArray(new String[0]))[0].equals("*"))) {
      _selectionColumns.clear();
      _selectionColumns.addAll(indexSegment.getColumnNames());
    }
    if (_selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : _selection.getSelectionSortSequence()) {
        _selectionColumns.add(selectionSort.getColumn());
      }
    }
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    ProjectionBlock projectionBlock;
    while ((projectionBlock = _projectionOperator.nextBlock()) != null) {
      for (int i = 0; i < _dataSchema.size(); i++) {
        _blocks[i] = projectionBlock.getBlock(_dataSchema.getColumnName(i));
      }
      DocIdSetBlock docIdSetBlock = projectionBlock.getDocIdSetBlock();
      _selectionOperatorService.iterateOnBlocksWithOrdering(docIdSetBlock.getBlockDocIdSet().iterator(), _blocks);
    }

    // Create execution statistics.
    numDocsScanned += _selectionOperatorService.getNumDocsScanned();
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumProjectionColumns();
    long numTotalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            numTotalRawDocs);

    return new IntermediateResultsBlock(_selectionOperatorService.getDataSchema(), _selectionOperatorService.getRows());
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
