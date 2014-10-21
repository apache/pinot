package com.linkedin.pinot.core.operator.query;

import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.block.intarray.DocIdSetBlock;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;


/**
 * This MSelectionOperator will take care of applying a selection query to one IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 * @author xiafu
 *
 */
public class MSelectionOperator implements Operator {

  private final IndexSegment _indexSegment;
  private final Operator _projectionOperator;
  private final Selection _selection;
  private final SelectionOperatorService _selectionOperatorService;
  private final DataSchema _dataSchema;
  private final BlockValSet[] _blockValSets;
  private final Set<String> _selectionColumns = new HashSet<String>();

  public MSelectionOperator(IndexSegment indexSegment, Selection selection, Operator projectionOperator) {
    _indexSegment = indexSegment;
    _selection = selection;
    _projectionOperator = projectionOperator;

    initColumnarDataSourcePlanNodeMap(indexSegment);
    _selectionOperatorService = new SelectionOperatorService(_selection, indexSegment);
    _dataSchema = _selectionOperatorService.getDataSchema();
    _blockValSets = new BlockValSet[_selectionColumns.size()];
  }

  private void initColumnarDataSourcePlanNodeMap(IndexSegment indexSegment) {
    _selectionColumns.addAll(_selection.getSelectionColumns());
    if ((_selectionColumns.size() == 1) && ((_selectionColumns.toArray(new String[0]))[0].equals("*"))) {
      _selectionColumns.clear();
      _selectionColumns.addAll(indexSegment.getSegmentMetadata().getSchema().getColumnNames());
    }
    if (_selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : _selection.getSelectionSortSequence()) {
        _selectionColumns.add(selectionSort.getColumn());
      }
    }
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public Block nextBlock() {

    final long startTime = System.currentTimeMillis();

    long numDocsScanned = 0;
    ProjectionBlock projectionBlock = null;
    while ((projectionBlock = (ProjectionBlock) _projectionOperator.nextBlock()) != null) {
      int j = 0;
      for (int i = 0; i < _dataSchema.size(); ++i) {
        if (_dataSchema.getColumnName(i).equalsIgnoreCase("_segmentId")
            || _dataSchema.getColumnName(i).equalsIgnoreCase("_docId")) {
          continue;
        }
        _blockValSets[j++] = projectionBlock.getBlock(_dataSchema.getColumnName(i)).getBlockValueSet();
      }

      _selectionOperatorService.iterateOnBlock(projectionBlock.getDocIdSetBlock().getBlockDocIdSet().iterator(),
          _blockValSets);
      numDocsScanned += ((DocIdSetBlock) (projectionBlock.getDocIdSetBlock())).getSearchableLength();
    }

    final IntermediateResultsBlock resultBlock = new IntermediateResultsBlock();
    resultBlock.setSelectionResult(_selectionOperatorService.getRowEventsSet());
    resultBlock.setSelectionDataSchema(_selectionOperatorService.getDataSchema());
    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalDocs(_indexSegment.getSegmentMetadata().getTotalDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTime);
    return resultBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }
}
