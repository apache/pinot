package org.apache.pinot.query.runtime.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


public class SortOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "SORT";
  private final BaseOperator<TransferableBlock> _upstreamOperator;
  private final DataSchema _dataSchema;
  private final PriorityQueue<Object[]> _rows;
  private final int _numRowsToKeep;

  private boolean _isSortedBlockConstructed;
  private TransferableBlock _upstreamErrorBlock;

  public SortOperator(BaseOperator<TransferableBlock> upstreamOperator, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, DataSchema dataSchema) {
    _upstreamOperator = upstreamOperator;
    _dataSchema = dataSchema;
    _upstreamErrorBlock = null;
    _isSortedBlockConstructed = false;
    _numRowsToKeep = SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY;
    _rows = new PriorityQueue<>(_numRowsToKeep, SelectionOperatorUtils.getTypeCompatibleComparator(
        toOrderByExpressions(collationKeys, collationDirections), dataSchema, false));
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      consumeInputBlocks();
      return produceSortedBlock();
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock produceSortedBlock()
      throws IOException {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    if (!_isSortedBlockConstructed) {
      int numColumns = _dataSchema.size();
      List<Object[]> rows = new ArrayList<>(_rows.size());
      while (_rows.size() > 0) {
        Object[] row = _rows.poll();
        rows.add(row);
      }
      if (rows.size() == 0) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
      } else {
        return new TransferableBlock(rows, _dataSchema, BaseDataBlock.Type.ROW);
      }
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
    }
  }

  private void consumeInputBlocks() {
    if (!_isSortedBlockConstructed) {
      TransferableBlock block = _upstreamOperator.nextBlock();
      while (!TransferableBlockUtils.isEndOfStream(block)) {
        BaseDataBlock dataBlock = block.getDataBlock();
        int numRows = dataBlock.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataBlock, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
        }
        block = _upstreamOperator.nextBlock();
      }
      // setting upstream error block
      if (block.isErrorBlock()) {
        _upstreamErrorBlock = block;
      }
    }
  }

  private List<OrderByExpressionContext> toOrderByExpressions(List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections) {
    List<OrderByExpressionContext> orderByExpressionContextList = new ArrayList<>(collationKeys.size());
    for (int i = 0; i < collationKeys.size(); i++) {
      orderByExpressionContextList.add(new OrderByExpressionContext(ExpressionContext.forIdentifier(
          _dataSchema.getColumnName(((RexExpression.InputRef) collationKeys.get(i)).getIndex())),
          !collationDirections.get(i).isDescending()));
    }
    return orderByExpressionContextList;
  }
}
