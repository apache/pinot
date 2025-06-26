package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


// TODO: implement
// TODO: whether to sort in this operator? or require input to be sorted?
public class MergeJoinOperator extends BaseJoinOperator {
  private static final String EXPLAIN_NAME = "MERGE_JOIN";

  // Placeholder for BitSet in _matchedRightRows when all keys are unique in the right table.
  private static final BitSet BIT_SET_PLACEHOLDER = new BitSet(0);

  private LinkedList<Object[]> _rightTable;
  private LinkedList<Object[]> _rightLookahead;

  private LinkedList<Object[]> _leftTable;
  private LinkedList<Object[]> _leftLookahead;

  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;

  private final Comparator<Object[]> _comparator;

  private MseBlock.Eos _leftEos;
  private MseBlock.Eos _rightEos;

  private int _numRowsJoined;

  public MergeJoinOperator(OpChainExecutionContext context,
      MultiStageOperator leftInput, DataSchema leftSchema, MultiStageOperator rightInput,
      JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);

    List<Integer> leftKeys = node.getLeftKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Merge join operator requires join keys");
    _leftKeySelector = KeySelectorFactory.getKeySelector(leftKeys);
    List<Integer> rightKeys = node.getRightKeys();
    Preconditions.checkState(!rightKeys.isEmpty(), "Merge join operator requires join keys");
    _rightKeySelector = KeySelectorFactory.getKeySelector(rightKeys);

    _comparator = SortUtils.SortComparator(node.getCollations(), false);

    _numRowsJoined = 0;
  }

  @Override
  protected MseBlock getNextBlock() {
    while (true) {
      if (_eos != null) {
        onEosProduced();
        return _eos;
      }
      getNextKeyGroup(_leftLookahead, _leftKeySelector, _leftTable, _leftInput, true);
      // either side produce EOS.Error, terminate
      if (_leftEos != null && _leftEos.isError()) {
        onEosProduced();
        return _leftEos;
      }
      getNextKeyGroup(_rightLookahead, _rightKeySelector, _rightTable, _rightInput, false);
      if (_rightEos != null && _rightEos.isError()) {
        onEosProduced();
        return _rightEos;
      }
      // if either side is empty we are done
      if (_leftTable.isEmpty() || _rightTable.isEmpty()) {
        onEosProduced();
        return SuccessMseBlock.INSTANCE;
      }
      List<Object[]> resultRows = joinKeyGroup();
      // either side produce EOS.Success, join current group with rest of the other
      if (!resultRows.isEmpty()) {
        return new RowHeapDataBlock(resultRows, _resultSchema);
      }
    }
  }

  /**
   * propagate key group with the next group of rows to be joined
   * @param lookahead buffer for keeping fetched but not processed rows
   * @param keySelector join key selector
   * @param keyGroup buffer for next group of rows to be joined, should be of the same key
   * @param input input operator
   * @param leftEos whether to propagate _leftEos or _rightEos
   */
  protected void getNextKeyGroup(LinkedList<Object[]> lookahead, KeySelector<?> keySelector,
      LinkedList<Object[]> keyGroup, MultiStageOperator input, boolean leftEos) {
    // if current keyGroup is not advanced, no-op
    if (!keyGroup.isEmpty()) {
      return;
    }
    Object groupKey = null;
    MseBlock.Eos eos = leftEos ? _leftEos : _rightEos;
    while (true) {
      while (!lookahead.isEmpty()) {
        if (groupKey == null) {
          Object[] row = lookahead.pollFirst();
          groupKey = keySelector.getKey(row);
          keyGroup.add(row);
          continue;
        }
        // if next key is not in the same group, group is done
        if (!groupKey.equals(keySelector.getKey(lookahead.getFirst()))) {
          return;
        }
        keyGroup.add(lookahead.pollFirst());
      }
      Preconditions.checkState(eos == null); // this should be unreachable if this side already Eos
      // lookahead is done but group is not, fetch more
      MseBlock nextBlock = input.nextBlock();
      // if we can't fetch, either error or done, at this time lookahead is also cleared
      // but there might be a group to join in keyGroup
      if (nextBlock.isEos()) {
        if (leftEos) {
          _leftEos = (MseBlock.Eos) nextBlock;
        } else {
          _rightEos = (MseBlock.Eos) nextBlock;
        }
        return;
      }
      // if we can fetch, continue building
      MseBlock.Data dataBlock = (MseBlock.Data) nextBlock;
      lookahead.addAll(dataBlock.asRowHeap().getRows());
    }
  }

  protected List<Object[]> joinKeyGroup() {
    Object[] leftFirstRow = _leftTable.getFirst();
    Object[] rightFirstRow = _rightTable.getFirst();
    Object leftKey = _leftKeySelector.getKey(leftFirstRow);
    Object rightKey = _rightKeySelector.getKey(rightFirstRow);
    if (!Objects.equals(leftKey, rightKey)) {
      // advance the smaller or equal group (collation is subset of join key)
      if (_comparator.compare(leftFirstRow, rightFirstRow) <= 0) {
        _leftTable.clear();
      } else {
        _rightTable.clear();
      }
      return Collections.emptyList();
    }
    // key matches, return cartesian product
    List<Object[]> resultRows = new ArrayList<>();
    for (Object[] leftRow : _leftTable) {
      for (Object[] rightRow : _rightTable) {
        List<Object> joinedRowView = joinRowView(leftRow, rightRow);
        if (matchNonEquiConditions(joinedRowView)) {
          if (isMaxRowsLimitReached(_numRowsJoined++)) {
            // if overflow break, return current joined rows
            // left is terminated in isMaxRowsLimitReached, also terminate right input
            earlyTerminateRightInput();
            return resultRows;
          }
          resultRows.add(joinedRowView.toArray());
        }
      }
    }
    // advance both sides
    _leftTable.clear();
    _rightTable.clear();
    return resultRows;
  }

  protected void earlyTerminateRightInput() {
    _rightInput.earlyTerminate();
    MseBlock rightBlock = _rightInput.nextBlock();

    while (rightBlock.isData()) {
      rightBlock = _rightInput.nextBlock();
    }
    _rightEos = (MseBlock.Eos) rightBlock;
  }

  @Override
  protected void onEosProduced() {
    // clear cached rows
    _leftLookahead = null;
    _leftTable = null;
    _rightLookahead = null;
    _rightTable = null;
  }

  // unused
  @Override
  protected void addRowsToRightTable(List<Object[]> rows) {
  }

  // unused
  @Override
  protected void finishBuildingRightTable() {
  }

  // unused
  @Override
  protected List<Object[]> buildJoinedRows(MseBlock.Data leftBlock) {
    return List.of();
  }

  // unused
  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    return List.of();
  }

  @Override
  public @Nullable String toExplainString() {
    return EXPLAIN_NAME;
  }
}
