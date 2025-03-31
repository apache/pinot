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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code LookupJoinOperator} implements the lookup join algorithm.
 * <p>This algorithm assumes that the right table is a dimension table which is preloaded. For each of the left table
 * row, it looks up for the corresponding row from the dimension table and create a joint row.
 * <p>For each of the data block received from the left table, it generates a joint data block. The output is in the
 * format of [left_row, right_row].
 * <p>Since right table is a dimension table which is replicated across all servers, RIGHT and FULL join are not
 * supported to avoid duplication.
 */
public class LookupJoinOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LookupJoinOperator.class);
  private static final String EXPLAIN_NAME = "LOOKUP_JOIN";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      Set.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.SEMI, JoinRelType.ANTI);

  private final MultiStageOperator _leftInput;
  private final LeafStageTransferableBlockOperator _rightInput;
  private final JoinRelType _joinType;
  private final int[] _leftKeyIds;
  private final DimensionTableDataManager _rightTable;
  private final String[] _rightColumns;
  private final DataSchema _resultSchema;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public LookupJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _leftInput = leftInput;
    Preconditions.checkState(rightInput instanceof LeafStageTransferableBlockOperator,
        "Right input must be leaf stage operator");
    _rightInput = (LeafStageTransferableBlockOperator) rightInput;
    _joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(_joinType), "Join type: % is not supported for lookup join",
        _joinType);

    List<Integer> leftKeys = node.getLeftKeys();
    _leftKeyIds = new int[leftKeys.size()];
    for (int i = 0; i < leftKeys.size(); i++) {
      _leftKeyIds[i] = leftKeys.get(i);
    }
    List<ServerQueryRequest> leafStageRequests = _rightInput.getRequests();
    Preconditions.checkState(leafStageRequests.size() == 1, "Lookup join cannot be applied to hybrid tables");
    QueryContext queryContext = leafStageRequests.get(0).getQueryContext();
    String rightTableName = queryContext.getTableName();
    _rightTable = DimensionTableDataManager.getInstanceByTableName(rightTableName);
    Preconditions.checkState(_rightTable != null, "Failed to find dimension table for name: %s", rightTableName);
    _rightColumns = _rightInput.getDataSchema().getColumnNames();
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(LookupJoinOperator.StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(LookupJoinOperator.StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.LOOKUP_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      TransferableBlock leftBlock = _leftInput.nextBlock();
      if (leftBlock.isErrorBlock()) {
        return leftBlock;
      }
      if (leftBlock.isSuccessfulEndOfStreamBlock()) {
        MultiStageQueryStats leftStats = leftBlock.getQueryStats();
        assert leftStats != null;
        leftStats.mergeInOrder(_rightInput.getQueryStats(), getOperatorType(), _statMap);
        return leftBlock;
      }
      assert leftBlock.isDataBlock();
      List<Object[]> rows = buildJoinedRows(leftBlock);
      sampleAndCheckInterruption();
      if (!rows.isEmpty()) {
        return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
      }
    }
  }

  private List<Object[]> buildJoinedRows(TransferableBlock leftBlock) {
    switch (_joinType) {
      case SEMI:
        return buildJoinedDataBlockSemi(leftBlock);
      case ANTI:
        return buildJoinedDataBlockAnti(leftBlock);
      default: { // INNER, LEFT, RIGHT, FULL
        return buildJoinedDataBlockDefault(leftBlock);
      }
    }
  }

  private List<Object[]> buildJoinedDataBlockDefault(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    ArrayList<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      PrimaryKey key = getKey(leftRow);
      Object[] rightRow = _rightTable.lookupValues(key, _rightColumns);
      if (rightRow != null) {
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (_nonEquiEvaluators.isEmpty() || _nonEquiEvaluators.stream()
            .allMatch(evaluator -> BooleanUtils.isTrueInternalValue(evaluator.apply(resultRow)))) {
          rows.add(resultRow);
          continue;
        }
      }
      if (needUnmatchedLeftRows()) {
        rows.add(joinRow(leftRow, null));
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockSemi(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());
    PrimaryKey key = new PrimaryKey(new Object[_leftKeyIds.length]);

    for (Object[] leftRow : container) {
      fillKey(leftRow, key);
      if (_rightTable.containsKey(key)) {
        rows.add(leftRow);
      }
    }
    return rows;
  }

  private List<Object[]> buildJoinedDataBlockAnti(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());
    PrimaryKey key = new PrimaryKey(new Object[_leftKeyIds.length]);

    for (Object[] leftRow : container) {
      fillKey(leftRow, key);
      if (!_rightTable.containsKey(key)) {
        rows.add(leftRow);
      }
    }
    return rows;
  }

  private PrimaryKey getKey(Object[] row) {
    Object[] values = new Object[_leftKeyIds.length];
    for (int i = 0; i < _leftKeyIds.length; i++) {
      values[i] = row[_leftKeyIds[i]];
    }
    return new PrimaryKey(values);
  }

  private void fillKey(Object[] row, PrimaryKey key) {
    Object[] values = key.getValues();
    for (int i = 0; i < _leftKeyIds.length; i++) {
      values[i] = row[_leftKeyIds[i]];
    }
  }

  private Object[] joinRow(Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultColumnSize];
    System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
    if (rightRow != null) {
      System.arraycopy(rightRow, 0, resultRow, leftRow.length, rightRow.length);
    }
    return resultRow;
  }

  private boolean needUnmatchedLeftRows() {
    return _joinType == JoinRelType.LEFT;
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    };
    //@formatter:on

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
