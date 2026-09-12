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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.join.JoinedRowView;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This `LookupJoinOperator` implements the lookup join algorithm.
///
/// This algorithm assumes that the right table is a dimension table which is preloaded. For each of the left table
/// row, it looks up for the corresponding row from the dimension table and create a joint row.
///
/// For each of the data block received from the left table, it generates a joint data block. The output is in the
/// format of \[left_row, right_row\].
///
/// Since right table is a dimension table which is replicated across all servers, RIGHT and FULL join are not
/// supported to avoid duplication.
///
/// The dimension table is a hash map keyed by the primary key values, so the lookup key must contain one value per
/// primary key column, in the order the dimension table schema declares them. The join condition does not provide the
/// key in that shape:
///
/// - The equi-join keys are ordered by the join condition, not by the primary key.
/// - A primary key column can be constrained by a constant (`dim.col = 'x'`) instead of by an equi-join key. Calcite
///   classifies such a condition as a non-equi condition, so it is absent from the join keys.
///
/// The constructor therefore compiles a key plan that maps every primary key column to its value source, and rejects
/// join conditions that cannot produce a complete key. See [#compileKeyPlan].
public class LookupJoinOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LookupJoinOperator.class);
  private static final String EXPLAIN_NAME = "LOOKUP_JOIN";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      Set.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.SEMI, JoinRelType.ANTI);

  /// Marks a key position that no join condition binds. It is an error for one to survive [#compileKeyPlan].
  private static final int KEY_SOURCE_UNBOUND = -1;
  /// Marks a key position whose value is a constant held in [#_keyConstants].
  private static final int KEY_SOURCE_CONSTANT = -2;

  private final MultiStageOperator _leftInput;
  private final int _leftColumnSize;
  private final LeafOperator _rightInput;
  private final JoinRelType _joinType;
  private final DimensionTableDataManager _rightTable;
  private final String[] _rightColumns;
  private final DataSchema _resultSchema;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  /// Number of primary key columns of the dimension table, i.e. the size of every lookup key.
  private final int _keySize;
  /// Value source of each key position: a left row index, or [#KEY_SOURCE_CONSTANT]. [#compileKeyPlan] rejects the
  /// plan if any position is still [#KEY_SOURCE_UNBOUND], so that value never reaches the probe path.
  private final int[] _keySources;
  /// Constant value of each key position whose source is [#KEY_SOURCE_CONSTANT].
  private final Object[] _keyConstants;
  /// Set when a constant key value is null. A null never matches a primary key, so every lookup misses.
  private final boolean _neverMatches;

  public LookupJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _leftInput = leftInput;
    _leftColumnSize = leftSchema.size();
    Preconditions.checkState(rightInput instanceof LeafOperator, "Right input must be leaf operator");
    _rightInput = (LeafOperator) rightInput;
    _joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(_joinType), "Join type: % is not supported for lookup join",
        _joinType);

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
    // SEMI and ANTI joins project the left columns only, so an evaluator built over the join result schema cannot
    // reference a dimension table column. Reject the combination here, otherwise the loop below fails with an index
    // error that says nothing about the cause.
    Preconditions.checkState(nonEquiConditions.isEmpty() || _joinType.projectsRight(),
        "Lookup join type: %s does not support non-equi join conditions, got: %s", _joinType, nonEquiConditions);
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }

    KeyPlan keyPlan =
        compileKeyPlan(node, rightTableName, _rightTable.getPrimaryKeyColumns(), _rightInput.getDataSchema(),
            _leftColumnSize);
    _keySize = keyPlan._sources.length;
    _keySources = keyPlan._sources;
    _keyConstants = keyPlan._constants;
    _neverMatches = keyPlan._neverMatches;
  }

  /// Works out where each value of the lookup key comes from.
  ///
  /// The key has one position per dimension table primary key column, in the order the dimension table schema declares
  /// them. Each position is bound in two passes:
  ///
  /// 1. Equi-join keys. `rightKeys[i]` names a dimension column, and that column's position in the primary key decides
  ///    where `leftKeys[i]` lands. This is what makes the key independent of the order of the join condition.
  /// 2. Constants. A non-equi condition of the form `dim_column = literal` binds a position that pass 1 left open.
  ///    A constant never replaces an equi-join key, because the equi-join key is not kept anywhere else and dropping it
  ///    would silently widen the join. A constant that pass 1 already bound stays in [#_nonEquiEvaluators] and runs as
  ///    a filter after the lookup, which is what the SQL semantics require.
  ///
  /// The method rejects a join condition that cannot produce exactly one value per primary key column. Every rejected
  /// case returned no rows or wrong rows before this validation existed, so an error is the better outcome. This is
  /// also the contract that the single-stage `lookup` transform function enforces.
  @VisibleForTesting
  static KeyPlan compileKeyPlan(JoinNode node, String tableName, @Nullable List<String> primaryKeyColumns,
      DataSchema rightSchema, int leftColumnSize) {
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Failed to find primary key columns for dimension table: %s", tableName);
    String[] rightColumns = rightSchema.getColumnNames();
    int keySize = primaryKeyColumns.size();
    int[] sources = new int[keySize];
    Arrays.fill(sources, KEY_SOURCE_UNBOUND);
    Object[] constants = new Object[keySize];

    // Pass 1: bind key positions from the equi-join keys.
    List<Integer> leftKeys = node.getLeftKeys();
    List<Integer> rightKeys = node.getRightKeys();
    int numEquiKeys = leftKeys.size();
    for (int i = 0; i < numEquiKeys; i++) {
      String rightColumn = rightColumns[rightKeys.get(i)];
      int keyPosition = primaryKeyColumns.indexOf(rightColumn);
      Preconditions.checkState(keyPosition >= 0,
          "Lookup join on dimension table: %s has a join key on column: %s, which is not a primary key column. "
              + "Primary key columns: %s", tableName, rightColumn, primaryKeyColumns);
      Preconditions.checkState(sources[keyPosition] == KEY_SOURCE_UNBOUND,
          "Lookup join on dimension table: %s has multiple join keys on primary key column: %s", tableName,
          rightColumn);
      sources[keyPosition] = leftKeys.get(i);
    }

    // Pass 2: bind the remaining key positions from constant equality conditions.
    boolean neverMatches = false;
    for (RexExpression nonEquiCondition : node.getNonEquiConditions()) {
      int rightColumnId = getConstantEqualityColumnId(nonEquiCondition, leftColumnSize, rightColumns.length);
      if (rightColumnId < 0) {
        continue;
      }
      int keyPosition = primaryKeyColumns.indexOf(rightColumns[rightColumnId]);
      if (keyPosition < 0 || sources[keyPosition] != KEY_SOURCE_UNBOUND) {
        continue;
      }
      sources[keyPosition] = KEY_SOURCE_CONSTANT;
      RexExpression.Literal literal = getConstantLiteral(nonEquiCondition);
      Object value = literal.getValue();
      if (value != null) {
        checkConstantType(literal, rightSchema.getColumnDataType(rightColumnId), tableName,
            rightColumns[rightColumnId]);
      }
      constants[keyPosition] = value;
      neverMatches |= value == null;
    }

    List<String> unboundColumns = new ArrayList<>();
    for (int i = 0; i < keySize; i++) {
      if (sources[i] == KEY_SOURCE_UNBOUND) {
        unboundColumns.add(primaryKeyColumns.get(i));
      }
    }
    Preconditions.checkState(unboundColumns.isEmpty(),
        "Lookup join on dimension table: %s cannot determine primary key columns: %s from the join condition. "
            + "A lookup join reads the dimension table by primary key, so the join condition must have an equality on "
            + "every primary key column: %s. Add the missing conditions, or remove the lookup join hint to use a hash "
            + "join instead.", tableName, unboundColumns, primaryKeyColumns);
    return new KeyPlan(sources, constants, neverMatches);
  }

  /// Returns the dimension table column id of a `dim_column = literal` condition, or -1 when the condition does not
  /// have that shape. Only an equality against a single literal can serve as a key value. A condition such as
  /// `dim_column IN ('a', 'b')` reaches this method as a disjunction and returns -1, because a hash lookup cannot read
  /// a set of keys.
  ///
  /// Non-equi conditions index the joined row, so the dimension table columns start at `leftColumnSize`.
  private static int getConstantEqualityColumnId(RexExpression condition, int leftColumnSize, int numRightColumns) {
    if (!(condition instanceof RexExpression.FunctionCall)) {
      return -1;
    }
    RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) condition;
    if (!functionCall.getFunctionName().equals(SqlKind.EQUALS.name())) {
      return -1;
    }
    List<RexExpression> operands = functionCall.getFunctionOperands();
    if (operands.size() != 2) {
      return -1;
    }
    RexExpression inputRef = operands.get(0) instanceof RexExpression.InputRef ? operands.get(0) : operands.get(1);
    RexExpression literal = operands.get(0) instanceof RexExpression.InputRef ? operands.get(1) : operands.get(0);
    if (!(inputRef instanceof RexExpression.InputRef) || !(literal instanceof RexExpression.Literal)) {
      return -1;
    }
    int columnId = ((RexExpression.InputRef) inputRef).getIndex() - leftColumnSize;
    return columnId >= 0 && columnId < numRightColumns ? columnId : -1;
  }

  /// Returns the literal of a condition that [#getConstantEqualityColumnId] accepted.
  private static RexExpression.Literal getConstantLiteral(RexExpression condition) {
    List<RexExpression> operands = ((RexExpression.FunctionCall) condition).getFunctionOperands();
    return (RexExpression.Literal) (operands.get(0) instanceof RexExpression.Literal ? operands.get(0)
        : operands.get(1));
  }

  /// Makes sure that a constant can serve as a lookup key value for the given dimension table column.
  ///
  /// The operator does not convert the constant. The planner coerces the operands of a comparison, so a literal
  /// compared against a dimension column already carries the type of that column. This check states that assumption,
  /// and fails with the table and column named if the planner ever stops holding it up. A constant of another type
  /// would miss every row, because [PrimaryKey] compares values with `equals` and an `Integer` never equals a `Long`.
  ///
  /// BYTES is rejected outright. A dimension table reads its key values with
  /// [org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader], which returns a raw `byte[]` whose
  /// `equals` and `hashCode` are identity. No constant of any representation can match such a key, so an error is
  /// better than an empty result. Fixing that belongs in `DimensionTableDataManager`, which would fix the single-stage
  /// `lookup` transform function at the same time.
  ///
  /// BIG_DECIMAL is allowed. `BigDecimal#equals` compares the scale, so a constant of `1.5` does not match a stored
  /// `1.50`, but a hash join carries the same hazard through [org.apache.pinot.query.runtime.operator.join.LookupTable]
  /// and this operator does not single out one arm of it.
  private static void checkConstantType(RexExpression.Literal literal, ColumnDataType columnDataType, String tableName,
      String column) {
    ColumnDataType storedType = columnDataType.getStoredType();
    Preconditions.checkState(storedType != ColumnDataType.BYTES,
        "Lookup join on dimension table: %s does not support a constant on primary key column: %s of type BYTES. "
            + "Remove the lookup join hint to use a hash join instead.", tableName, column);
    Preconditions.checkState(literal.getDataType().getStoredType() == storedType,
        "Lookup join on dimension table: %s got a constant of stored type: %s on primary key column: %s of stored "
            + "type: %s", tableName, literal.getDataType().getStoredType(), column, storedType);
  }

  /// Value sources of the lookup key, one entry per dimension table primary key column.
  ///
  /// `_sources` holds a left row index, or [#KEY_SOURCE_CONSTANT] when the value is in `_constants` at the same
  /// position. `_neverMatches` is set when a constant is null, which no primary key value equals.
  @VisibleForTesting
  static class KeyPlan {
    final int[] _sources;
    final Object[] _constants;
    final boolean _neverMatches;

    KeyPlan(int[] sources, Object[] constants, boolean neverMatches) {
      _sources = sources;
      _constants = constants;
      _neverMatches = neverMatches;
    }
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
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
  protected MseBlock getNextBlock() {
    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      MseBlock leftBlock = _leftInput.nextBlock();
      if (leftBlock.isEos()) {
        return leftBlock;
      }
      List<Object[]> rows = buildJoinedRows((MseBlock.Data) leftBlock);
      checkTerminationAndSampleUsage();
      if (!rows.isEmpty()) {
        return new RowHeapDataBlock(rows, _resultSchema);
      }
    }
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  private List<Object[]> buildJoinedRows(MseBlock.Data leftBlock) {
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

  private List<Object[]> buildJoinedDataBlockDefault(MseBlock.Data leftBlock) {
    List<Object[]> container = leftBlock.asRowHeap().getRows();
    ArrayList<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Object[] rightRow = _neverMatches ? null : _rightTable.lookupValues(getKey(leftRow), _rightColumns);
      if (rightRow != null) {
        List<Object> resultRow = JoinedRowView.of(leftRow, rightRow, _resultColumnSize, _leftColumnSize);
        if (_nonEquiEvaluators.isEmpty() || _nonEquiEvaluators.stream()
            .allMatch(evaluator -> BooleanUtils.isTrueInternalValue(evaluator.apply(resultRow)))) {
          // defer copying of the content until row matches
          rows.add(resultRow.toArray());
          continue;
        }
      }
      if (needUnmatchedLeftRows()) {
        rows.add(joinRow(leftRow, null));
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockSemi(MseBlock.Data leftBlock) {
    List<Object[]> container = leftBlock.asRowHeap().getRows();
    // A constant key value only comes from a non-equi condition, which the constructor rejects for this join type, so
    // there is no null constant to short-circuit on here.
    List<Object[]> rows = new ArrayList<>(container.size());
    PrimaryKey key = new PrimaryKey(new Object[_keySize]);

    for (Object[] leftRow : container) {
      fillKey(leftRow, key);
      if (_rightTable.containsKey(key)) {
        rows.add(leftRow);
      }
    }
    return rows;
  }

  private List<Object[]> buildJoinedDataBlockAnti(MseBlock.Data leftBlock) {
    List<Object[]> container = leftBlock.asRowHeap().getRows();
    // See the note in buildJoinedDataBlockSemi on the absence of a null constant short-circuit.
    List<Object[]> rows = new ArrayList<>(container.size());
    PrimaryKey key = new PrimaryKey(new Object[_keySize]);

    for (Object[] leftRow : container) {
      fillKey(leftRow, key);
      if (!_rightTable.containsKey(key)) {
        rows.add(leftRow);
      }
    }
    return rows;
  }

  private PrimaryKey getKey(Object[] row) {
    Object[] values = new Object[_keySize];
    fillKeyValues(row, values);
    return new PrimaryKey(values);
  }

  private void fillKey(Object[] row, PrimaryKey key) {
    fillKeyValues(row, key.getValues());
  }

  private void fillKeyValues(Object[] row, Object[] values) {
    for (int i = 0; i < _keySize; i++) {
      int source = _keySources[i];
      values[i] = source == KEY_SOURCE_CONSTANT ? _keyConstants[i] : row[source];
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
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG);

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
