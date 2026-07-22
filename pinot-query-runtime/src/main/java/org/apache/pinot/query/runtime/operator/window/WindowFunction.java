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
package org.apache.pinot.query.runtime.operator.window;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;


/**
 * This class provides the basic structure for window functions. It provides the batch row processing API:
 * processRows(List<Object[]> rows) which processes a batch of rows at a time.
 *
 */
public abstract class WindowFunction extends AggregationUtils.Accumulator {
  protected final int[] _orderKeys;
  protected final int[] _inputRefs;
  protected final WindowFrame _windowFrame;
  // Metadata for the single ORDER BY key, needed to evaluate value-based RANGE offset frames. For RANGE offset frames
  // Calcite guarantees exactly one ORDER BY key, so these describe that key. Null / default when there is no ORDER BY.
  @Nullable
  protected final ColumnDataType _orderKeyStoredType;
  protected final boolean _orderKeyAscending;

  public WindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema, List<RelFieldCollation> collations,
      WindowFrame windowFrame) {
    super(aggCall, inputSchema);
    int numOrderKeys = collations.size();
    _orderKeys = new int[numOrderKeys];
    for (int i = 0; i < numOrderKeys; i++) {
      _orderKeys[i] = collations.get(i).getFieldIndex();
    }
    _windowFrame = windowFrame;
    if (numOrderKeys > 0) {
      RelFieldCollation firstCollation = collations.get(0);
      _orderKeyStoredType = inputSchema.getColumnDataType(firstCollation.getFieldIndex()).getStoredType();
      _orderKeyAscending = !firstCollation.getDirection().isDescending();
    } else {
      _orderKeyStoredType = null;
      _orderKeyAscending = true;
    }
    if (WindowAggregateOperator.RANKING_FUNCTION_NAMES.contains(aggCall.getFunctionName())) {
      _inputRefs = _orderKeys;
    } else {
      _inputRefs = new int[]{_inputRef};
    }
  }

  /**
   * Computes the inclusive base frame bounds {@code {starts, ends}} for a value-based RANGE offset frame over the given
   * partition rows, via an O(n) two-pointer sweep. Only valid for RANGE offset frames with a single numeric ORDER BY
   * key. See {@link RangeWindowFrameBounds}. Any EXCLUDE clause is applied on top of these base bounds by the caller.
   */
  protected int[][] computeRangeFrameBounds(List<Object[]> rows) {
    return RangeWindowFrameBounds.compute(rows, _orderKeys[0], _orderKeyStoredType, _orderKeyAscending, _windowFrame);
  }

  /**
   * Batch processing API for Window functions.
   * This method processes a batch of rows at a time.
   * Each row generates one object as output.
   * Note, the input and output list size should be the same.
   *
   * @param rows List of rows to process
   * @return List of rows with the window function applied
   */
  public abstract List<Object> processRows(List<Object[]> rows);

  protected Object extractValueFromRow(Object[] row) {
    return _inputRef == -1 ? _literal : (row == null ? null : row[_inputRef]);
  }

  /**
   * Returns whether peer-group information is required to apply the given EXCLUDE clause. {@code CURRENT_ROW} only
   * touches the current row, so callers can skip the O(n) peer-boundary computation when the frame doesn't otherwise
   * depend on peer bounds (i.e. ROWS frames).
   */
  protected static boolean needsPeerBoundaries(WindowNode.WindowExclusion exclude) {
    return exclude == WindowNode.WindowExclusion.GROUP || exclude == WindowNode.WindowExclusion.TIES;
  }

  /**
   * Fills {@code peerStart} and {@code peerEnd} with the inclusive bounds of each row's peer group based on the
   * ORDER BY keys. Rows are peers iff they share the same ORDER BY values. With no ORDER BY clause every row is a peer
   * of every other row in the partition.
   */
  protected void computePeerBoundaries(List<Object[]> rows, int[] peerStart, int[] peerEnd) {
    int numRows = rows.size();
    if (_orderKeys.length == 0) {
      for (int i = 0; i < numRows; i++) {
        peerStart[i] = 0;
        peerEnd[i] = numRows - 1;
      }
      return;
    }
    int groupStart = 0;
    for (int i = 1; i <= numRows; i++) {
      if (i == numRows || !samePeerKey(rows.get(i - 1), rows.get(i))) {
        int peerLast = i - 1;
        for (int j = groupStart; j < i; j++) {
          peerStart[j] = groupStart;
          peerEnd[j] = peerLast;
        }
        groupStart = i;
      }
    }
  }

  private boolean samePeerKey(Object[] a, Object[] b) {
    for (int k : _orderKeys) {
      if (!Objects.equals(a[k], b[k])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the first index in {@code [fs, fe]} that is not excluded by the SQL EXCLUDE clause for the current row
   * {@code i} whose peer group is {@code [pStart, pEnd]}. Returns {@code -1} if no such index exists.
   */
  protected static int firstNonExcluded(int fs, int fe, int i, int pStart, int pEnd,
      WindowNode.WindowExclusion exclude) {
    if (fs > fe) {
      return -1;
    }
    switch (exclude) {
      case CURRENT_ROW:
        return fs == i ? (fs + 1 > fe ? -1 : fs + 1) : fs;
      case GROUP:
        if (fs < pStart || fs > pEnd) {
          return fs;
        }
        return pEnd + 1 > fe ? -1 : pEnd + 1;
      case TIES:
        if (fs == i || fs < pStart || fs > pEnd) {
          return fs;
        }
        if (i >= fs && i <= fe) {
          return i;
        }
        return pEnd + 1 > fe ? -1 : pEnd + 1;
      default:
        return fs;
    }
  }

  /**
   * Returns the inclusive lower index of the base frame for the row at index {@code i} given its peer group
   * {@code [pStart, pEnd]} and the total {@code numRows} in the partition.
   */
  protected int frameStartForRow(int i, int pStart, int numRows) {
    if (_windowFrame.isRowType()) {
      int lb = _windowFrame.getLowerBound();
      return lb == Integer.MIN_VALUE ? 0 : Math.max(0, lb + i);
    }
    return _windowFrame.isUnboundedPreceding() ? 0 : pStart;
  }

  /**
   * Returns the inclusive upper index of the base frame for the row at index {@code i} given its peer group
   * {@code [pStart, pEnd]} and the total {@code numRows} in the partition.
   */
  protected int frameEndForRow(int i, int pEnd, int numRows) {
    if (_windowFrame.isRowType()) {
      int ub = _windowFrame.getUpperBound();
      return ub == Integer.MAX_VALUE ? numRows - 1 : Math.min(numRows - 1, ub + i);
    }
    return _windowFrame.isUnboundedFollowing() ? numRows - 1 : pEnd;
  }

  /**
   * Returns the last index in {@code [fs, fe]} that is not excluded by the SQL EXCLUDE clause for the current row
   * {@code i} whose peer group is {@code [pStart, pEnd]}. Returns {@code -1} if no such index exists.
   */
  protected static int lastNonExcluded(int fs, int fe, int i, int pStart, int pEnd,
      WindowNode.WindowExclusion exclude) {
    if (fs > fe) {
      return -1;
    }
    switch (exclude) {
      case CURRENT_ROW:
        return fe == i ? (fe - 1 < fs ? -1 : fe - 1) : fe;
      case GROUP:
        if (fe < pStart || fe > pEnd) {
          return fe;
        }
        return pStart - 1 < fs ? -1 : pStart - 1;
      case TIES:
        if (fe == i || fe < pStart || fe > pEnd) {
          return fe;
        }
        if (i >= fs && i <= fe) {
          return i;
        }
        return pStart - 1 < fs ? -1 : pStart - 1;
      default:
        return fe;
    }
  }
}
