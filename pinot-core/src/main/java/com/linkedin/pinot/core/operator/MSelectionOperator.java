package com.linkedin.pinot.core.operator;

import java.util.Map;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;


/**
 * This UAggregationAndSelectionOperator will take care of applying a request
 * with both aggregation and selection to one IndexSegment.
 *
 * @author xiafu
 *
 */
public class MSelectionOperator implements Operator {

  private final IndexSegment _indexSegment;
  private final BIndexSegmentProjectionOperator _projectionOperator;
  private final Map<String, Operator> _columnarDataSourceMap;
  private final Selection _selections;
  private final SelectionOperatorService _selectionOperatorService;
  private final DataSchema _dataSchema;
  private final BlockValIterator[] _blockValIterators;

  public MSelectionOperator(IndexSegment indexSegment, Selection selections, Operator operator,
      Map<String, Operator> columnarDataSourceMap) {
    _indexSegment = indexSegment;
    _projectionOperator = (BIndexSegmentProjectionOperator) operator;
    _selections = selections;
    _columnarDataSourceMap = columnarDataSourceMap;
    _selectionOperatorService = new SelectionOperatorService(_selections, indexSegment);
    _dataSchema = _selectionOperatorService.getDataSchema();
    _blockValIterators = new BlockValIterator[_columnarDataSourceMap.size()];

  }

  @Override
  public boolean open() {
    if (_projectionOperator != null) {
      _projectionOperator.open();
    }
    for (Operator op : _columnarDataSourceMap.values()) {
      op.open();
    }
    return true;
  }

  @Override
  public Block nextBlock() {

    final long startTime = System.currentTimeMillis();

    long numDocsScanned = 0;
    while (_projectionOperator.nextBlock() != null) {
      int j = 0;
      for (int i = 0; i < _dataSchema.size(); ++i) {
        if (_dataSchema.getColumnName(i).equalsIgnoreCase("_segmentId")
            || _dataSchema.getColumnName(i).equalsIgnoreCase("_docId")) {
          continue;
        }
        _blockValIterators[j++] =
            _columnarDataSourceMap.get(_dataSchema.getColumnName(i)).nextBlock().getBlockValueSet().iterator();
      }
      _selectionOperatorService.iterateOnBlock(_projectionOperator.getCurrentDocIdSetBlock().getBlockDocIdSet()
          .iterator(), _blockValIterators);
      numDocsScanned += _projectionOperator.getCurrentBlockSize();
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
    if (_projectionOperator != null) {
      _projectionOperator.close();
    }
    for (Operator op : _columnarDataSourceMap.values()) {
      op.close();
    }
    return true;
  }
}
