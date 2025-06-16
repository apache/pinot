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
import org.apache.pinot.common.utils.DataSchema;
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

  public HashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);
    List<Integer> leftKeys = node.getLeftKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Hash join operator requires join keys");
    _leftKeySelector = KeySelectorFactory.getKeySelector(leftKeys);
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
    _rightTable = createLookupTable(leftKeys, leftSchema);
    _matchedRightRows = needUnmatchedRightRows() ? new HashMap<>() : null;
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
      _rightTable.addRow(_rightKeySelector.getKey(row), row);
    }
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

  private List<Object[]> buildJoinedDataBlockUniqueKeys(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    ArrayList<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      Object[] rightRow = (Object[]) _rightTable.lookup(key);
      if (rightRow == null) {
        handleUnmatchedLeftRow(leftRow, rows);
      } else {
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (matchNonEquiConditions(resultRow)) {
          if (isMaxRowsLimitReached(rows.size())) {
            break;
          }
          rows.add(resultRow);
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
      Object key = _leftKeySelector.getKey(leftRow);
      List<Object[]> rightRows = (List<Object[]>) _rightTable.lookup(key);
      if (rightRows == null) {
        handleUnmatchedLeftRow(leftRow, rows);
      } else {
        boolean maxRowsLimitReached = false;
        boolean hasMatchForLeftRow = false;
        int numRightRows = rightRows.size();
        for (int i = 0; i < numRightRows; i++) {
          Object[] resultRow = joinRow(leftRow, rightRows.get(i));
          if (matchNonEquiConditions(resultRow)) {
            if (isMaxRowsLimitReached(rows.size())) {
              maxRowsLimitReached = true;
              break;
            }
            rows.add(resultRow);
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
      rows.add(joinRow(leftRow, null));
    }
  }

  private List<Object[]> buildJoinedDataBlockSemi(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      // SEMI-JOIN only checks existence of the key
      if (_rightTable.containsKey(key)) {
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
      // ANTI-JOIN only checks non-existence of the key
      if (!_rightTable.containsKey(key)) {
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
          rows.add(joinRow(null, rightRow));
        }
      }
    } else {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        List<Object[]> rightRows = ((List<Object[]>) entry.getValue());
        BitSet matchedIndices = _matchedRightRows.get(entry.getKey());
        if (matchedIndices == null) {
          for (Object[] rightRow : rightRows) {
            rows.add(joinRow(null, rightRow));
          }
        } else {
          int numRightRows = rightRows.size();
          int unmatchedIndex = 0;
          while ((unmatchedIndex = matchedIndices.nextClearBit(unmatchedIndex)) < numRightRows) {
            rows.add(joinRow(null, rightRows.get(unmatchedIndex++)));
          }
        }
      }
    }
    return rows;
  }
}
