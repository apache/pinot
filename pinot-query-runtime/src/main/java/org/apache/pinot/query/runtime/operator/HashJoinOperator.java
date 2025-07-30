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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.join.DoubleLookupTable;
import org.apache.pinot.query.runtime.operator.join.FloatLookupTable;
import org.apache.pinot.query.runtime.operator.join.IntLookupTable;
import org.apache.pinot.query.runtime.operator.join.LongLookupTable;
import org.apache.pinot.query.runtime.operator.join.LookupTable;
import org.apache.pinot.query.runtime.operator.join.ObjectLookupTable;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.trace.Tracing;


/**
 * This {@code HashJoinOperator} join algorithm with join keys. Right table is materialized into a hash table.
 */
// TODO: Support memory size based resource limit.
@SuppressWarnings("unchecked")
public class HashJoinOperator extends BaseJoinOperator {
  private static final String EXPLAIN_NAME = "HASH_JOIN";

  // Placeholder for BitSet in _matchedRightRows when all keys are unique in the right table.
  private static final BitSet BIT_SET_PLACEHOLDER = new BitSet(0);

  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  @Nullable
  private LookupTable _rightTable;
  // Track matched right rows for right join and full join to output non-matched right rows.
  // TODO: Revisit whether we should use IntList or RoaringBitmap for smaller memory footprint.
  // TODO: Optimize this
  @Nullable
  private Map<Object, BitSet> _matchedRightRows;
  // Store null key rows separately for RIGHT and FULL JOINs
  @Nullable
  private List<Object[]> _nullKeyRightRows;

  public HashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);
    List<Integer> leftKeys = node.getLeftKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Hash join operator requires join keys");
    _leftKeySelector = KeySelectorFactory.getKeySelector(leftKeys);
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
    _rightTable = createLookupTable(leftKeys, leftSchema);
    _matchedRightRows = needUnmatchedRightRows() ? new HashMap<>() : null;
    // Initialize _nullKeyRightRows for both RIGHT and FULL JOINs
    _nullKeyRightRows = needUnmatchedRightRows() ? new ArrayList<>() : null;
  }

  private static LookupTable createLookupTable(List<Integer> joinKeys, DataSchema schema) {
    if (joinKeys.size() > 1) {
      return new ObjectLookupTable();
    }
    switch (schema.getColumnDataType(joinKeys.get(0)).getStoredType()) {
      case INT:
        return new IntLookupTable();
      case LONG:
        return new LongLookupTable();
      case FLOAT:
        return new FloatLookupTable();
      case DOUBLE:
        return new DoubleLookupTable();
      default:
        return new ObjectLookupTable();
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void addRowsToRightTable(List<Object[]> rows) {
    assert _rightTable != null : "Right table should not be null when adding rows";
    for (Object[] row : rows) {
      Object key = _rightKeySelector.getKey(row);
      // Skip rows with null join keys - they should not participate in equi-joins per SQL standard
      if (isNullKey(key)) {
        // For RIGHT and FULL JOIN, we need to preserve null key rows for the final output
        if (_nullKeyRightRows != null) {
          _nullKeyRightRows.add(row);
        }
        continue;
      }
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(_rightTable.size());
      _rightTable.addRow(key, row);
    }
  }

  /**
   * Check if a join key contains null values. In SQL standard, null keys should not match in equi-joins.
   **/
  private boolean isNullKey(Object key) {
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

  @Override
  protected void finishBuildingRightTable() {
    assert _rightTable != null : "Right table should not be null when finishing building";
    _rightTable.finish();
  }

  @Override
  protected void onEosProduced() {
    _rightTable = null;
    _matchedRightRows = null;
    _nullKeyRightRows = null;
  }

  @Override
  protected List<Object[]> buildJoinedRows(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    switch (_joinType) {
      case SEMI:
        return buildJoinedDataBlockSemi(leftBlock);
      case ANTI:
        return buildJoinedDataBlockAnti(leftBlock);
      default: { // INNER, LEFT, RIGHT, FULL
        if (_rightTable.isKeysUnique()) {
          return buildJoinedDataBlockUniqueKeys(leftBlock);
        } else {
          return buildJoinedDataBlockDuplicateKeys(leftBlock);
        }
      }
    }
  }

  private boolean handleNullKey(Object key, Object[] leftRow, List<Object[]> rows) {
    if (isNullKey(key)) {
      // For INNER joins, don't add anything when key is null
      if (_joinType == JoinRelType.LEFT || _joinType == JoinRelType.FULL) {
        handleUnmatchedLeftRow(leftRow, rows);
      }
      return true;
    }
    return false;
  }

  private List<Object[]> buildJoinedDataBlockUniqueKeys(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    ArrayList<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      // Skip rows with null join keys - they should not participate in equi-joins per SQL standard
      if (handleNullKey(key, leftRow, rows)) {
        continue;
      }
      Object[] rightRow = (Object[]) _rightTable.lookup(key);
      if (rightRow == null) {
        handleUnmatchedLeftRow(leftRow, rows);
      } else {
        List<Object> resultRowView = joinRowView(leftRow, rightRow);
        if (matchNonEquiConditions(resultRowView)) {
          if (isMaxRowsLimitReached(rows.size())) {
            break;
          }
          // defer copying of the content until row matches
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
          rows.add(resultRowView.toArray());
          if (_matchedRightRows != null) {
            _matchedRightRows.put(key, BIT_SET_PLACEHOLDER);
          }
        } else {
          handleUnmatchedLeftRow(leftRow, rows);
        }
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockDuplicateKeys(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
      Object key = _leftKeySelector.getKey(leftRow);
      // Skip rows with null join keys - they should not participate in equi-joins per SQL standard
      if (handleNullKey(key, leftRow, rows)) {
        continue;
      }
      List<Object[]> rightRows = (List<Object[]>) _rightTable.lookup(key);
      if (rightRows == null) {
        handleUnmatchedLeftRow(leftRow, rows);
      } else {
        boolean maxRowsLimitReached = false;
        boolean hasMatchForLeftRow = false;
        int numRightRows = rightRows.size();
        for (int i = 0; i < numRightRows; i++) {
          List<Object> resultRowView = joinRowView(leftRow, rightRows.get(i));
          if (matchNonEquiConditions(resultRowView)) {
            if (isMaxRowsLimitReached(rows.size())) {
              maxRowsLimitReached = true;
              break;
            }
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
            rows.add(resultRowView.toArray());
            hasMatchForLeftRow = true;
            if (_matchedRightRows != null) {
              _matchedRightRows.computeIfAbsent(key, k -> new BitSet(numRightRows)).set(i);
            }
          }
        }
        if (maxRowsLimitReached) {
          break;
        }
        if (!hasMatchForLeftRow) {
          handleUnmatchedLeftRow(leftRow, rows);
        }
      }
    }

    return rows;
  }

  private void handleUnmatchedLeftRow(Object[] leftRow, List<Object[]> rows) {
    if (needUnmatchedLeftRows()) {
      if (isMaxRowsLimitReached(rows.size())) {
        return;
      }
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
      rows.add(joinRow(leftRow, null));
    }
  }

  private List<Object[]> buildJoinedDataBlockSemi(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      if (_rightTable.containsKey(key)) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
        rows.add(leftRow);
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockAnti(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      if (!_rightTable.containsKey(key)) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
        rows.add(leftRow);
      }
    }

    return rows;
  }

  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    assert _rightTable != null : "Right table should not be null when building non-matched right rows";
    assert _matchedRightRows != null : "Matched right rows should not be null when building non-matched right rows";
    List<Object[]> rows = new ArrayList<>();
    if (_rightTable.isKeysUnique()) {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        Object[] rightRow = (Object[]) entry.getValue();
        if (!_matchedRightRows.containsKey(entry.getKey())) {
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
          rows.add(joinRow(null, rightRow));
        }
      }
    } else {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        List<Object[]> rightRows = ((List<Object[]>) entry.getValue());
        BitSet matchedIndices = _matchedRightRows.get(entry.getKey());
        if (matchedIndices == null) {
          for (Object[] rightRow : rightRows) {
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
            rows.add(joinRow(null, rightRow));
          }
        } else {
          int numRightRows = rightRows.size();
          int unmatchedIndex = 0;
          while ((unmatchedIndex = matchedIndices.nextClearBit(unmatchedIndex)) < numRightRows) {
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
            rows.add(joinRow(null, rightRows.get(unmatchedIndex++)));
          }
        }
      }
    }
    // Add unmatched null key rows from right side for RIGHT and FULL JOIN
    if (_nullKeyRightRows != null) {
      for (Object[] nullKeyRow : _nullKeyRightRows) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
        rows.add(joinRow(null, nullKeyRow));
      }
    }
    return rows;
  }
}
