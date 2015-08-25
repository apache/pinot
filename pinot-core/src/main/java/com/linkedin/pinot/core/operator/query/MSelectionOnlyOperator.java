/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import com.linkedin.pinot.core.operator.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * This MSelectionOnlyOperator will take care of applying a selection query to one IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 *
 */
public class MSelectionOnlyOperator extends BaseOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MSelectionOnlyOperator.class);

  private final IndexSegment _indexSegment;
  private final Operator _projectionOperator;
  private final Selection _selection;
  private final DataSchema _dataSchema;
  private final Block[] _blocks;
  private final String[] _selectionColumns;
  private final int _limitDocs;
  private final Collection<Serializable[]> _rowEvents;

  public MSelectionOnlyOperator(IndexSegment indexSegment, Selection selection, Operator projectionOperator) {
    _indexSegment = indexSegment;
    _selection = selection;
    _limitDocs = _selection.getSize();
    _projectionOperator = projectionOperator;

    _selectionColumns = SelectionOperatorUtils.extractSelectionRelatedColumns(_selection, _indexSegment);
    _dataSchema = SelectionOperatorUtils.extractDataSchema(_selectionColumns, indexSegment);
    _blocks = new Block[_selectionColumns.length];
    _rowEvents = new ArrayList<Serializable[]>();
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public Block getNextBlock() {

    final long startTime = System.currentTimeMillis();
    long numDocsScanned = 0;
    ProjectionBlock projectionBlock = null;
    while ((projectionBlock = (ProjectionBlock) _projectionOperator.nextBlock()) != null) {
      int j = 0;
      for (int i = 0; i < _dataSchema.size(); ++i) {
        _blocks[j++] = projectionBlock.getBlock(_dataSchema.getColumnName(i));
      }
      BlockDocIdIterator blockDocIdIterator = projectionBlock.getDocIdSetBlock().getBlockDocIdSet().iterator();
      int docId;
      while ((docId = blockDocIdIterator.next()) != Constants.EOF && _rowEvents.size() < _limitDocs) {
        numDocsScanned++;
        _rowEvents.add(SelectionOperatorUtils.collectRowFromBlockValSets(docId, _blocks, _dataSchema));
      }
      if (_rowEvents.size() == _limitDocs) {
        break;
      }
    }

    final IntermediateResultsBlock resultBlock = new IntermediateResultsBlock();
    resultBlock.setSelectionResult(_rowEvents);
    resultBlock.setSelectionDataSchema(_dataSchema);
    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalDocs(_indexSegment.getTotalDocs());
    final long endTime = System.currentTimeMillis();
    resultBlock.setTimeUsedMs(endTime - startTime);
    return resultBlock;

  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "MSelectionOnlyOperator";
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }
}
