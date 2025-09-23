package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.BaseJoinOperator.StatKey;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;

import static org.apache.pinot.query.runtime.operator.BaseJoinOperator.DEFAULT_JOIN_OVERFLOW_MODE;
import static org.apache.pinot.query.runtime.operator.BaseJoinOperator.DEFAULT_MAX_ROWS_IN_JOIN;
import static org.apache.pinot.query.runtime.operator.BaseJoinOperator.LOGGER;


/**
 * Assumes input is sorted appropriately, and runs a merge join algorithm.
 * TODO: I am skipping past duplicates right now.
 */
public class SortedMergeJoinOperator extends MultiStageOperator {
  private final MultiStageOperator _leftInput;
  private final MultiStageOperator _rightInput;
  private final JoinRelType _joinRelType;
  protected final DataSchema _resultSchema;
  protected final int _leftColumnSize;
  protected final int _resultColumnSize;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  /**
   * Max rows allowed to build the right table hash collection. Also max rows emitted in each join with a block from
   * the left table.
   */
  private final int _maxRowsInJoin;
  /**
   * Mode when join overflow happens, supported values: THROW or BREAK.
   *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
   *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
   */
  private final JoinOverFlowMode _joinOverflowMode;
  @Nullable
  private MseBlock.Eos _eos;

  public SortedMergeJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _leftInput = leftInput;
    _rightInput = rightInput;
    _joinRelType = node.getJoinType();
    _resultSchema = node.getDataSchema();
    _leftColumnSize = node.getInputs().get(0).getDataSchema().size();
    _resultColumnSize = _resultSchema.size();
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = getJoinOverflowMode(metadata, nodeHint);
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.HASH_JOIN;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  protected MseBlock getNextBlock()
      throws Exception {
    if (_eos != null) {
      return _eos;
    }
    switch (_joinRelType) {
      case LEFT:
        return computeLeftJoin();
      default:
        throw new UnsupportedOperationException("");
    }
  }

  protected MseBlock computeLeftJoin() {
    MseBlock leftBlock = _leftInput.nextBlock();
    MseBlock rightBlock = _rightInput.nextBlock();
    if (leftBlock.isError()) {
      _eos = (MseBlock.Eos) leftBlock;
      return _eos;
    }
    if (rightBlock.isError()) {
      _eos = (MseBlock.Eos) rightBlock;
      return _eos;
    }
    List<Object[]> leftRows = leftBlock.isData() ? ((MseBlock.Data) leftBlock).asRowHeap().getRows() : null;
    List<Object[]> rightRows = rightBlock.isData() ? ((MseBlock.Data) rightBlock).asRowHeap().getRows() : null;
    int leftIndex = 0;
    int rightIndex = 0;
    Comparable leftKey = null;
    Comparable rightKey = null;
    List<Object[]> result = new ArrayList<>(10_000);
    while (leftRows != null && rightRows != null) {
      while (leftIndex < leftRows.size() && rightIndex < rightRows.size()) {
        leftKey = leftKey == null ? (Comparable) _leftKeySelector.getKey(leftRows.get(leftIndex)) : leftKey;
        rightKey = rightKey == null ? (Comparable) _rightKeySelector.getKey(rightRows.get(rightIndex)) : rightKey;
        if (isNullKey(rightKey)) {
          rightIndex++;
          rightKey = null;
          continue;
        } else if (isNullKey(leftKey)) {
          leftIndex++;
          leftKey = null;
          continue;
        }
        int c = leftKey.compareTo(rightKey);
        if (c == 0) {
          Object[] newRow = new Object[_resultColumnSize];
          System.arraycopy(leftRows.get(leftIndex), 0, newRow, 0, _leftColumnSize);
          System.arraycopy(rightRows.get(rightIndex), 0, newRow, _leftColumnSize, _resultColumnSize - _leftColumnSize);
          result.add(newRow);
          leftIndex++;
          rightIndex++;
          // Breeze past duplicates on left.
          Comparable newLeftKey = null;
          while (leftIndex < leftRows.size() && (newLeftKey = (Comparable) _leftKeySelector.getKey(leftRows.get(leftIndex))) != null) {
            if (!newLeftKey.equals(leftKey)) {
              break;
            }
            leftIndex++;
          }
          leftKey = newLeftKey;
          // Breeze past duplicates on right.
          Comparable newRightKey = null;
          while (rightIndex < rightRows.size() && (newRightKey = (Comparable) _rightKeySelector.getKey(rightRows.get(rightIndex))) != null) {
            if (!newRightKey.equals(rightKey)) {
              break;
            }
            rightIndex++;
          }
          rightKey = newRightKey;
        } else if (c > 0) {
          rightIndex++;
          rightKey = null;
        } else {
          leftIndex++;
          leftKey = null;
        }
      }
      if (leftIndex == leftRows.size()) {
        leftBlock = _leftInput.nextBlock();
        leftRows = leftBlock.isData() ? ((MseBlock.Data) leftBlock).asRowHeap().getRows() : null;
      }
      if (rightIndex == rightRows.size()) {
        rightBlock = _rightInput.nextBlock();
        rightRows = rightBlock.isData() ? ((MseBlock.Data) rightBlock).asRowHeap().getRows() : null;
      }
    }
    if (leftBlock.isError()) {
      _eos = (MseBlock.Eos) leftBlock;
      return _eos;
    } else if (rightBlock.isError()) {
      _eos = (MseBlock.Eos) rightBlock;
      return _eos;
    }
    // Drain left-input if required.
    while (leftRows != null) {
      while (leftIndex < leftRows.size()) {
        Object[] newRow = new Object[_resultColumnSize];
        System.arraycopy(leftRows.get(leftIndex), 0, newRow, 0, _leftColumnSize);
        result.add(newRow);
        leftIndex++;
      }
      leftBlock = _leftInput.nextBlock();
      leftRows = leftBlock.isData() ? ((MseBlock.Data) leftBlock).asRowHeap().getRows() : null;
    }
    if (leftBlock.isError()) {
      _eos = (MseBlock.Eos) leftBlock;
      return _eos;
    }
    // reaching here means that we have completed consumption on at least one side.
    return new RowHeapDataBlock(result, _resultSchema);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return "MERGE_JOIN";
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  protected static int getMaxRowsInJoin(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    return maxRowsInJoin != null ? maxRowsInJoin : DEFAULT_MAX_ROWS_IN_JOIN;
  }

  protected static JoinOverFlowMode getJoinOverflowMode(Map<String, String> contextMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(contextMetadata);
    return joinOverflowMode != null ? joinOverflowMode : DEFAULT_JOIN_OVERFLOW_MODE;
  }

  /**
   * Check if a join key contains null values. In SQL standard, null keys should not match in equi-joins.
   **/
  private static boolean isNullKey(Object key) {
    if (key == null) {
      return true;
    }
    if (key instanceof Key) {
      Object[] components = ((Key) key).getValues();
      for (Object comp : components) {
        if (comp == null) {
          return true;
        }
      }
      return false;
    }
    // For single keys (non-composite), key == null is already checked above
    return false;
  }
}
