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
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants;

@SuppressWarnings({"unchecked", "rawType"})
public class EnrichedHashJoinOperator extends HashJoinOperator {
  private static final String EXPLAIN_NAME = "ENRICHED_JOIN";
  private final List<FilterProjectOperand> _filterProjectOperands;
  private final int _resultColumnSize;
  private final int _numRowsToKeep;
  private int _rowsSeen = 0;
  private int _numRowsToOffset;

  public EnrichedHashJoinOperator(OpChainExecutionContext context,
      MultiStageOperator leftInput, DataSchema leftSchema, MultiStageOperator rightInput,
      EnrichedJoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node, node.getJoinResultSchema());

    DataSchema joinResultSchema = node.getJoinResultSchema();

    _resultColumnSize = joinResultSchema.size();

    // a chain of filter and project operands
    _filterProjectOperands = new ArrayList<>();
    DataSchema currentSchema = joinResultSchema;
    // iterate and convert filter and project rexes into operands with the correctly chained schema
    for (EnrichedJoinNode.FilterProjectRex rex : node.getFilterProjectRexes()) {
      _filterProjectOperands.add(new FilterProjectOperand(rex, currentSchema));
      currentSchema = rex.getProjectAndResultSchema() == null
          ? currentSchema : rex.getProjectAndResultSchema().getSchema();
    }

    int offset = Math.max(node.getOffset(), 0);
    int fetch = node.getFetch();

    // TODO: see if this need to be converted to input args
    int defaultResponseLimit = CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT;
    _numRowsToKeep = fetch > 0 ? fetch + offset : defaultResponseLimit;
    _numRowsToOffset = offset;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Filter, project on a joined row view
  private void filterProjectLimit(List<Object> rowView, List<Object[]> rows) {
    Object[] row = null;

    for (FilterProjectOperand filterProjectOperand : _filterProjectOperands) {
      if (filterProjectOperand.getType() == FilterProjectOperandsType.FILTER) {
        // use rowView before reaching a project
        assert (filterProjectOperand.getFilter() != null);
        if (row == null
            ? filterDiscardRow(rowView, filterProjectOperand.getFilter())
            : filterDiscardRow(row, filterProjectOperand.getFilter())
        ) {
          return;
        }
      } else {
        assert (filterProjectOperand.getProject() != null);
        row = row == null
            ? projectRow(rowView, filterProjectOperand.getProject())
            : projectRow(row, filterProjectOperand.getProject());
      }
    }

    if (rowNotNeeded()) {
      return;
    }

    // if filter only, materialize rowView at the end
    rows.add(row == null ? rowView.toArray() : row);
  }

  /// Filter, project, limit on the left and right row by creating a view
  private void filterProjectLimit(Object[] leftRow, Object[] rightRow, List<Object[]> rows,
      int resultColumnSize, int leftColumnSize) {
    List<Object> rowView = JoinedRowView.of(leftRow, rightRow, resultColumnSize, leftColumnSize);
    Object[] row = null;

    for (FilterProjectOperand filterProjectOperand : _filterProjectOperands) {
      if (filterProjectOperand.getType() == FilterProjectOperandsType.FILTER) {
        // use rowView before reaching a project
        assert (filterProjectOperand.getFilter() != null);
        if (row == null
            ? filterDiscardRow(rowView, filterProjectOperand.getFilter())
            : filterDiscardRow(row, filterProjectOperand.getFilter())
        ) {
          return;
        }
      } else {
        assert (filterProjectOperand.getProject() != null);
        row = row == null
            ? projectRow(rowView, filterProjectOperand.getProject())
            : projectRow(row, filterProjectOperand.getProject());
      }
    }

    if (rowNotNeeded()) {
      return;
    }

    // if filter only, materialize rowView at the end
    rows.add(row == null ? rowView.toArray() : row);
  }

  /// Limit on a row, return true if the limit reached before adding this row
  private boolean rowNotNeeded() {
    // limit only, terminate if enough rows
    if (_rowsSeen++ == _numRowsToKeep) {
      earlyTerminate();
      logger().debug("EnrichedHashJoinOperator: seen enough rows with no sort, early terminating");
      return true;
    }
    return false;
  }

  /// Filter a row by left and right child, return whether the row is discarded
  private boolean filterDiscardRow(List<Object> rowView, TransformOperand filter) {
    Object filterResult = filter.apply(rowView);
    return !BooleanUtils.isTrueInternalValue(filterResult);
  }

  private boolean filterDiscardRow(Object[] row, TransformOperand filter) {
    Object filterResult = filter.apply(row);
    return !BooleanUtils.isTrueInternalValue(filterResult);
  }

  /// Return the projected row
  private Object[] projectRow(List<Object> rowView, List<TransformOperand> project) {
    Object[] resultRow = new Object[project.size()];
    for (int i = 0; i < project.size(); i++) {
      resultRow[i] = project.get(i).apply(rowView);
    }
    return resultRow;
  }

  /// Return the projected row from input row
  private Object[] projectRow(Object[] row, List<TransformOperand> project) {
    Object[] resultRow = new Object[project.size()];
    for (int i = 0; i < project.size(); i++) {
      resultRow[i] = project.get(i).apply(row);
    }
    return resultRow;
  }

  /// Read result from _priorityQueue if sort needed, else return rows
  private List<Object[]> getOutputRows(List<Object[]> rows) {
    if (_numRowsToOffset <= 0) {
      return rows;
    }
    if (rows.size() > _numRowsToOffset) {
      int rowSize = rows.size();
      rows = rows.subList(_numRowsToOffset, rows.size());
      _numRowsToOffset -= rowSize;
      return rows;
    }
    _numRowsToOffset -= rows.size();
    return Collections.emptyList();
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
        filterProjectLimit(null, rightRow, rows, _resultColumnSize, _leftColumnSize);
      }
    } else {
      for (Map.Entry<Object, Object> entry : _rightTable.entrySet()) {
        List<Object[]> rightRows = ((List<Object[]>) entry.getValue());
        BitSet matchedIndices = _matchedRightRows.get(entry.getKey());
        if (matchedIndices == null) {
          for (Object[] rightRow : rightRows) {
            filterProjectLimit(null, rightRow, rows, _resultColumnSize, _leftColumnSize);
          }
        } else {
          int numRightRows = rightRows.size();
          int unmatchedIndex = 0;
          while ((unmatchedIndex = matchedIndices.nextClearBit(unmatchedIndex)) < numRightRows) {
            filterProjectLimit(null, rightRows.get(unmatchedIndex++), rows, _resultColumnSize, _leftColumnSize);
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
      filterProjectLimit(leftRow, null, rows, _resultColumnSize, _leftColumnSize);
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

  /// MatchNonEquiConditions that takes the row view
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
        List<Object> resultRowView = JoinedRowView.of(leftRow, rightRow, _resultColumnSize, _leftColumnSize);
        if (matchNonEquiConditions(resultRowView)) {
          if (isMaxRowsLimitReached(rows.size())) {
            break;
          }
          // filter project sortLimit on the produced row
          filterProjectLimit(resultRowView, rows);
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
          List<Object> resultRowView = JoinedRowView.of(leftRow, rightRows.get(i), _resultColumnSize, _leftColumnSize);
          if (matchNonEquiConditions(resultRowView)) {
            if (isMaxRowsLimitReached(rows.size())) {
              maxRowsLimitReached = true;
              break;
            }
            // filter project sortLimit on the produced row
            filterProjectLimit(resultRowView, rows);
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
        filterProjectLimit(leftRow, null, rows, _leftColumnSize, _leftColumnSize);
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
        filterProjectLimit(leftRow, null, rows, _leftColumnSize, _leftColumnSize);
      }
    }

    return getOutputRows(rows);
  }

  public enum FilterProjectOperandsType {
    FILTER,
    PROJECT
  }

  public static class FilterProjectOperand {
    private final FilterProjectOperandsType _type;
    @Nullable
    final TransformOperand _filter;
    @Nullable
    final List<TransformOperand> _project;

    public FilterProjectOperand(EnrichedJoinNode.FilterProjectRex rex, DataSchema inputSchema) {
      if (rex.getType() == EnrichedJoinNode.FilterProjectRexType.FILTER) {
        _type = FilterProjectOperandsType.FILTER;
        _filter = TransformOperandFactory.getTransformOperand(rex.getFilter(), inputSchema);
        _project = null;
      } else {
        _type = FilterProjectOperandsType.PROJECT;
        _filter = null;
        List<TransformOperand> projects = new ArrayList<>();
        assert (rex.getProjectAndResultSchema() != null);
        rex.getProjectAndResultSchema().getProject().forEach((x) ->
            projects.add(TransformOperandFactory.getTransformOperand(x, inputSchema)));
        _project = projects;
      }
    }

    public TransformOperand getFilter() {
      return _filter;
    }

    public List<TransformOperand> getProject() {
      return _project;
    }

    public FilterProjectOperandsType getType() {
      return _type;
    }
  }
}
