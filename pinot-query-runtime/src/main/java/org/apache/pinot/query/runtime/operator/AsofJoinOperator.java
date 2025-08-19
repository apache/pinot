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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.trace.Tracing;


public class AsofJoinOperator extends BaseJoinOperator {
  private static final String EXPLAIN_NAME = "ASOF_JOIN";

  // The right table is a map from the hash key (columns in the ON join condition) to a sorted map of match key
  // (column in the MATCH_CONDITION) to rows.
  @Nullable
  private Map<Object, NavigableMap<Comparable<?>, Object[]>> _rightTable;
  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  private final MatchConditionType _matchConditionType;
  private final int _leftMatchKeyIndex;
  private final int _rightMatchKeyIndex;

  public AsofJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);
    _rightTable = new HashMap<>();
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());

    RexExpression matchCondition = node.getMatchCondition();
    try {
      _matchConditionType =
          MatchConditionType.valueOf(((RexExpression.FunctionCall) matchCondition).getFunctionName().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException("Unsupported match condition: " + matchCondition);
    }

    List<RexExpression> matchKeys = ((RexExpression.FunctionCall) matchCondition).getFunctionOperands();
    _leftMatchKeyIndex = ((RexExpression.InputRef) matchKeys.get(0)).getIndex();
    _rightMatchKeyIndex = ((RexExpression.InputRef) matchKeys.get(1)).getIndex() - leftSchema.size();
  }

  @Override
  protected void addRowsToRightTable(List<Object[]> rows) {
    assert _rightTable != null : "Right table should not be null when adding rows";
    for (Object[] row : rows) {
      Comparable<?> matchKey = (Comparable<?>) row[_rightMatchKeyIndex];
      if (matchKey == null) {
        // Skip rows with null match keys because they cannot be matched with any left rows
        continue;
      }
      Object hashKey = _rightKeySelector.getKey(row);
      // Results need not be deterministic if there are "ties" based on the match key in an ASOF JOIN, so it's okay to
      // only keep the last row with the same hash key and match key.
      _rightTable.computeIfAbsent(hashKey, k -> new TreeMap<>()).put(matchKey, row);
    }
  }

  @Override
  protected void finishBuildingRightTable() {
    // no-op
  }

  @Override
  protected void onEosProduced() {
    _rightTable = null; // Release memory in case we keep the operator around for a while
  }

  @Override
  protected List<Object[]> buildJoinedRows(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> rows = new ArrayList<>();
    for (Object[] leftRow : leftBlock.asRowHeap().getRows()) {
      Comparable<?> matchKey = (Comparable<?>) leftRow[_leftMatchKeyIndex];
      if (matchKey == null) {
        // Rows with null match keys cannot be matched with any right rows
        if (needUnmatchedLeftRows()) {
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
          rows.add(joinRow(leftRow, null));
        }
        continue;
      }
      Object hashKey = _leftKeySelector.getKey(leftRow);
      NavigableMap<Comparable<?>, Object[]> rightRows = _rightTable.get(hashKey);
      if (rightRows == null) {
        if (needUnmatchedLeftRows()) {
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
          rows.add(joinRow(leftRow, null));
        }
      } else {
        Object[] rightRow = closestMatch(matchKey, rightRows);
        if (rightRow == null) {
          if (needUnmatchedLeftRows()) {
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
            rows.add(joinRow(leftRow, null));
          }
        } else {
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rows.size());
          rows.add(joinRow(leftRow, rightRow));
        }
      }
    }
    return rows;
  }

  @Nullable
  private Object[] closestMatch(Comparable<?> matchKey, NavigableMap<Comparable<?>, Object[]> rightRows) {
    switch (_matchConditionType) {
      case GREATER_THAN: {
        // Find the closest right row that is less than the left row (compared by their match keys from the match
        // condition).
        Map.Entry<Comparable<?>, Object[]> closestMatch = rightRows.lowerEntry(matchKey);
        return closestMatch == null ? null : closestMatch.getValue();
      }
      case GREATER_THAN_OR_EQUAL: {
        // Find the closest right row that is less than or equal to the left row (compared by their match keys from
        // the match condition).
        Map.Entry<Comparable<?>, Object[]> closestMatch = rightRows.floorEntry(matchKey);
        return closestMatch == null ? null : closestMatch.getValue();
      }
      case LESS_THAN: {
        // Find the closest right row that is greater than the left row (compared by their match keys from the match
        // condition).
        Map.Entry<Comparable<?>, Object[]> closestMatch = rightRows.higherEntry(matchKey);
        return closestMatch == null ? null : closestMatch.getValue();
      }
      case LESS_THAN_OR_EQUAL: {
        // Find the closest right row that is greater than or equal to the left row (compared by their match keys from
        // the match condition).
        Map.Entry<Comparable<?>, Object[]> closestMatch = rightRows.ceilingEntry(matchKey);
        return closestMatch == null ? null : closestMatch.getValue();
      }
      default:
        throw new IllegalArgumentException("Unsupported match condition type: " + _matchConditionType);
    }
  }

  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    // There's only ASOF JOIN and LEFT ASOF JOIN; RIGHT ASOF JOIN is not a thing
    throw new UnsupportedOperationException("ASOF JOIN does not support unmatched right rows");
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  private enum MatchConditionType {
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL
  }
}
