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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;


public class EnrichedHashJoinOperator extends HashJoinOperator {
  private static final String EXPLAIN_NAME = "ENRICHED_JOIN";
  @Nullable
  private final TransformOperand _filterOperand;
  @Nullable
  private final List<TransformOperand> _projectOperands;
  private final int _projectResultSize;
  private final DataSchema _joinResultSchema;
  // _projectResultSchema is currently not used because sort operation
  //    does not care about input schema
  private final DataSchema _projectResultSchema;
  private final int _resultColumnSize;

  public EnrichedHashJoinOperator(OpChainExecutionContext context,
      MultiStageOperator leftInput, DataSchema leftSchema, MultiStageOperator rightInput,
      EnrichedJoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node, node.getJoinResultSchema());

    _joinResultSchema = node.getJoinResultSchema();
    _projectResultSchema = node.getProjectResultSchema();

    _resultColumnSize = _joinResultSchema.size();

    // input of filter is join result
    _filterOperand = node.getFilterCondition() == null ?
        null : TransformOperandFactory.getTransformOperand(node.getFilterCondition(), _joinResultSchema);

    List<RexExpression> projectExpressions = node.getProjects();
    if (projectExpressions == null) {
      _projectOperands = null;
      // if no projection is done, result size is same as join output
      _projectResultSize = _resultColumnSize;
    } else {
      _projectOperands = new ArrayList<>();
      // input of project is filter result, which has same schema as join result
      projectExpressions.forEach( (x) -> {
          _projectOperands.add(TransformOperandFactory.getTransformOperand(x, _joinResultSchema));
        });
      _projectResultSize = projectExpressions.size();
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  // TODO: check null in advance and do code specialization
  /** filter a row by left and right child, return whether the row is kept */
  private boolean filterRow(List<Object> rowView) {
    if (_filterOperand == null) {
      return true;
    }
    Object filterResult = _filterOperand.apply(rowView);
    return BooleanUtils.isTrueInternalValue(filterResult);
  }

  /** return the projected rowView */
  private Object[] projectRow(List<Object> rowView) {
    if (_projectOperands == null) {
      return rowView.toArray();
    }
    Object[] resultRow = new Object[_projectResultSize];
    for (int i=0; i<_projectResultSize; i++) {
      resultRow[i] = _projectOperands.get(i).apply(rowView);
    }
    return resultRow;
  }

  /** read result from _priorityQueue if sort needed, else return rows */
  private List<Object[]> getOutputRows(List<Object[]> rows) {
    return rows;
  }

  /** filter, project on the left and right row by creating a view */
  private void filterProject(Object[] leftRow, Object[] rightRow, List<Object[]> rows,
      int resultColumnSize, int leftColumnSize) {
    // TODO: this should handle different orders of filter, project
    JoinedRowView rowView = new JoinedRowView(leftRow, rightRow, resultColumnSize, leftColumnSize);
    // filter
    if (!filterRow(rowView)) { return; }
    // project, this incurs one copy of the element
    Object[] joinedRow = projectRow(rowView);
    rows.add(joinedRow);
  }

  /** filter, project on a joined row view */
  private void filterProject(List<Object> rowView, List<Object[]> rows) {
    // filter
    if (!filterRow(rowView)) { return; }
    // project, this incurs one copy of the element
    Object[] joinedRow = projectRow(rowView);
    rows.add(joinedRow);
  }

  /**
   * Enriched version of buildNonMatchedRightRows that filter, project, sort-limit it
   * @return filter, projected, sort-limited rows
   */
  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    assert _rightTable != null : "Right table should not be null when building non-matched right rows";
    assert _matchedRightRows != null : "Matched right rows should not be null when building non-matched right rows";
    List<Object[]> rows = new ArrayList<>();
    if (_rightTable.isKeysUnique()) {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        Object[] rightRow = (Object[]) entry.getValue();
        if (_matchedRightRows.containsKey(entry.getKey())) {
          continue;
        }
        // join row with null, then project-merge-sort-limit
        filterProject(null, rightRow, rows, _resultColumnSize, _leftColumnSize);
      }
    } else {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        List<Object[]> rightRows = ((List<Object[]>) entry.getValue());
        BitSet matchedIndices = _matchedRightRows.get(entry.getKey());
        if (matchedIndices == null) {
          for (Object[] rightRow : rightRows) {
            filterProject(null, rightRow, rows, _resultColumnSize, _leftColumnSize);
          }
        } else {
          int numRightRows = rightRows.size();
          int unmatchedIndex = 0;
          while ((unmatchedIndex = matchedIndices.nextClearBit(unmatchedIndex)) < numRightRows) {
            filterProject(null, rightRows.get(unmatchedIndex++), rows, _resultColumnSize, _leftColumnSize);
          }
        }
      }
    }
    // return the result, fetch from pq if there's sort-limit
    return getOutputRows(rows);
  }

  private void handleUnmatchedLeftRow(Object[] leftRow, List<Object[]> rows) {
    if (needUnmatchedLeftRows()) {
      if (isMaxRowsLimitReached(rows.size())) {
        return;
      }
      filterProject(leftRow, null, rows, _resultColumnSize, _leftColumnSize);
    }
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

  /** matchNonEquiConditions that takes the row view */
  protected final boolean matchNonEquiConditions(List<Object> rowView) {
    if (_nonEquiEvaluators.isEmpty()) {
      return true;
    }
    for (TransformOperand evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(rowView))) {
        return false;
      }
    }
    return true;
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
        List<Object> resultRowView = new JoinedRowView(leftRow, rightRow, _resultColumnSize, _leftColumnSize);
        if (matchNonEquiConditions(resultRowView)) {
          if (isMaxRowsLimitReached(rows.size())) {
            break;
          }
          // filter project sortLimit on the produced row
          filterProject(resultRowView, rows);
          if (_matchedRightRows != null) {
            _matchedRightRows.put(key, BIT_SET_PLACEHOLDER);
          }
        } else {
          handleUnmatchedLeftRow(leftRow, rows);
        }
      }
    }

    return getOutputRows(rows);
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
          List<Object> resultRowView = new JoinedRowView(leftRow, rightRows.get(i), _resultColumnSize, _leftColumnSize);
          if (matchNonEquiConditions(resultRowView)) {
            if (isMaxRowsLimitReached(rows.size())) {
              maxRowsLimitReached = true;
              break;
            }
            // filter project sortLimit on the produced row
            filterProject(resultRowView, rows);
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

    return getOutputRows(rows);
  }

  private List<Object[]> buildJoinedDataBlockAnti(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      // ANTI-JOIN only checks non-existence of the key
      if (!_rightTable.containsKey(key)) {
        filterProject(leftRow, null, rows, _leftColumnSize, _leftColumnSize);
      }
    }

    return getOutputRows(rows);
  }

  private List<Object[]> buildJoinedDataBlockSemi(MseBlock.Data leftBlock) {
    assert _rightTable != null : "Right table should not be null when building joined rows";
    List<Object[]> leftRows = leftBlock.asRowHeap().getRows();
    List<Object[]> rows = new ArrayList<>(leftRows.size());

    for (Object[] leftRow : leftRows) {
      Object key = _leftKeySelector.getKey(leftRow);
      // SEMI-JOIN only checks existence of the key
      if (_rightTable.containsKey(key)) {
        filterProject(leftRow, null, rows, _leftColumnSize, _leftColumnSize);
      }
    }

    return getOutputRows(rows);
  }

  /**
   * This util class is a view over the left and right row joined together
   * currently this is used for filtering and input of projection. So if the joined
   * tuple doesn't pass the predicate, the join result is not materialized into Object[].
   *
   * It is debatable whether we always want to use this instead of copying the tuple
   */
  private static class JoinedRowView extends AbstractList<Object> implements List<Object> {
    @Nullable
    private final Object[] _leftRow;
    @Nullable
    private final Object[] _rightRow;
    private final int _leftSize;
    private final int _size;

    public JoinedRowView(@Nullable Object[] leftRow, @Nullable Object[] rightRow, int resultColumnSize, int leftSize) {
      _leftRow = leftRow;
      _rightRow = rightRow;
      _leftSize = leftSize;
      _size = resultColumnSize;
    }

    @Override
    public Object get(int i) {
      return i < _leftSize ? (_leftRow == null ? null : _leftRow[i]) : (_rightRow == null ? null : _rightRow[i - _leftSize]);
    }

    @Override
    public int size() {
      return _size;
    }

    /** materialize the view into a row array */
    @Override
    @NotNull
    public Object[] toArray() {
      Object[] resultRow = new Object[_size];
      if (_leftRow != null) {
        System.arraycopy(_leftRow, 0, resultRow, 0, _leftSize);
      }
      if (_rightRow != null) {
        System.arraycopy(_rightRow, 0, resultRow, _leftSize, _rightRow.length);
      }
      return resultRow;
    }
  }
}
