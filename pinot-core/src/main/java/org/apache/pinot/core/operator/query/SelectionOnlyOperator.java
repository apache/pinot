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

import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.query.selection.SelectionFetcher;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.query.selection.SelectionFetcher;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * This SelectionOnlyOperator will take care of applying a selection query to one IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 *
 */
public class SelectionOnlyOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "SelectionOnlyOperator";

  private final IndexSegment _indexSegment;
  private final ProjectionOperator _projectionOperator;
  private final DataSchema _dataSchema;
  private final Block[] _blocks;
  private final int _limitDocs;
  private final Collection<Serializable[]> _rowEvents;
  private ExecutionStatistics _executionStatistics;

  public SelectionOnlyOperator(IndexSegment indexSegment, Selection selection, ProjectionOperator projectionOperator) {
    _indexSegment = indexSegment;
    _limitDocs = selection.getSize();
    _projectionOperator = projectionOperator;
    List<String> selectionColumns =
        SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), indexSegment);
    _dataSchema = SelectionOperatorUtils.extractDataSchema(null, selectionColumns, indexSegment);
    _blocks = new Block[selectionColumns.size()];
    _rowEvents = new ArrayList<>();
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    ProjectionBlock projectionBlock;
    while ((projectionBlock = _projectionOperator.nextBlock()) != null) {
      for (int i = 0; i < _dataSchema.size(); i++) {
        _blocks[i] = projectionBlock.getBlock(_dataSchema.getColumnName(i));
      }
      SelectionFetcher selectionFetcher = new SelectionFetcher(_blocks, _dataSchema);
      DocIdSetBlock docIdSetBlock = projectionBlock.getDocIdSetBlock();
      int numDocsToFetch = Math.min(docIdSetBlock.getSearchableLength(), _limitDocs - _rowEvents.size());
      numDocsScanned += numDocsToFetch;
      int[] docIdSet = docIdSetBlock.getDocIdSet();
      for (int i = 0; i < numDocsToFetch; i++) {
        _rowEvents.add(selectionFetcher.getRow(docIdSet[i]));
      }
      if (_rowEvents.size() == _limitDocs) {
        break;
      }
    }

    // Create execution statistics.
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumColumnsProjected();
    long numTotalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            numTotalRawDocs);

    return new IntermediateResultsBlock(_dataSchema, _rowEvents);
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
