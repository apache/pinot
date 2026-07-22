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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;


public class AggregateWindowFunction extends WindowFunction {
  private final WindowValueAggregator<Object> _windowValueAggregator;
  private final String _functionName;

  public AggregateWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
    _functionName = aggCall.getFunctionName();
    // Removal support is required for sliding ROWS frames and whenever an EXCLUDE clause forces per-row corrections.
    boolean nonDefaultExclude = !windowFrame.isExcludeNoOthers();
    // Removal is also required for value-based RANGE offset frames, which slide over the ORDER BY value.
    boolean supportRemoval = nonDefaultExclude || windowFrame.isRangeOffsetFrame() || (windowFrame.isRowType() && !(
        windowFrame.isUnboundedPreceding() && windowFrame.isUnboundedFollowing()));
    _windowValueAggregator = WindowValueAggregatorFactory.getWindowValueAggregator(_functionName, _dataType,
        supportRemoval, nonDefaultExclude);
  }

  @Override
  public final List<Object> processRows(List<Object[]> rows) {
    _windowValueAggregator.clear();
    if (_windowFrame.isExcludeNoOthers()) {
      return _windowFrame.isRowType() ? processRowsWindow(rows) : processRangeWindow(rows);
    }
    return _windowFrame.isRowType() ? processRowsWindowWithExclude(rows) : processRangeWindowWithExclude(rows);
  }

  /**
   * Process windows where both ends are unbounded. Both ROWS and RANGE windows can be processed similarly.
   */
  private List<Object> processUnboundedPrecedingAndFollowingWindow(List<Object[]> rows) {
    // Process all rows at once
    for (Object[] row : rows) {
      _windowValueAggregator.addValue(extractValueFromRow(row));
    }
    return Collections.nCopies(rows.size(), _windowValueAggregator.getCurrentAggregatedValue());
  }

  private List<Object> processRowsWindow(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedPrecedingAndFollowingWindow(rows);
    }

    int numRows = rows.size();

    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    // Add elements from first window
    for (int i = Math.max(0, lowerBound); i <= upperBound; i++) {
      _windowValueAggregator.addValue(extractValueFromRow(rows.get(i)));
    }

    List<Object> result = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        // Fill the remaining rows with null since all subsequent windows will be out of bounds
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        return result;
      }
      result.add(_windowValueAggregator.getCurrentAggregatedValue());

      // Slide the window forward by one
      if (lowerBound >= 0) {
        _windowValueAggregator.removeValue(extractValueFromRow(rows.get(lowerBound)));
      }
      lowerBound++;

      if (upperBound < numRows - 1) {
        upperBound++;
        if (upperBound >= 0) {
          _windowValueAggregator.addValue(extractValueFromRow(rows.get(upperBound)));
        }
      }
    }
    return result;
  }

  private List<Object> processRangeWindow(List<Object[]> rows) {
    // We don't currently support RANGE windows with offset FOLLOWING / PRECEDING and this is validated during planning
    // so we can safely assume that the lower bound is either UNBOUNDED PRECEDING or CURRENT ROW and the upper bound
    // is either UNBOUNDED FOLLOWING or CURRENT ROW.

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedPrecedingAndFollowingWindow(rows);
    }

    List<Object> results = new ArrayList<>(rows.size());
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUpperBoundCurrentRow()) {
      // The window frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW - this means that the result for rows
      // with the same order key will be the same - equal to the aggregated result from the first row of the partition
      // to the last row with that order key.
      Map<Key, Object> keyedResult = new HashMap<>();
      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        _windowValueAggregator.addValue(extractValueFromRow(row));
        keyedResult.put(orderKey, _windowValueAggregator.getCurrentAggregatedValue());
      }

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        results.add(keyedResult.get(orderKey));
      }
      return results;
    } else if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUnboundedFollowing()) {
      // The window frame is RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING - this means that the result for rows
      // with the same order key will be the same - equal to the aggregated result from the first row with that order
      // key to the last row of the partition.
      Map<Key, Object> keyedResult = new HashMap<>();
      // Do a reverse iteration
      for (int i = rows.size() - 1; i >= 0; i--) {
        Object[] row = rows.get(i);
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        _windowValueAggregator.addValue(extractValueFromRow(row));
        keyedResult.put(orderKey, _windowValueAggregator.getCurrentAggregatedValue());
      }

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        results.add(keyedResult.get(orderKey));
      }
      return results;
    } else if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUpperBoundCurrentRow()) {
      // The window frame is RANGE BETWEEN CURRENT ROW AND CURRENT ROW - this means that the result for rows with the
      // same order key will be the same - equal to the aggregated result from the first row with that order key to the
      // last row with that order key.
      Map<Key, WindowValueAggregator<Object>> keyedAggregator = new HashMap<>();
      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        keyedAggregator.computeIfAbsent(orderKey,
                k -> WindowValueAggregatorFactory.getWindowValueAggregator(_functionName, _dataType, false))
            .addValue(extractValueFromRow(row));
      }

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        results.add(keyedAggregator.get(orderKey).getCurrentAggregatedValue());
      }
      return results;
    } else {
      // Value-based RANGE offset frame (e.g. RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) without an EXCLUDE clause.
      return processRangeOffsetWindow(rows);
    }
  }

  /**
   * Processes a value-based RANGE offset frame by sliding an incremental aggregator over the base frame bounds computed
   * from the ORDER BY value. Both frame endpoints advance monotonically, so this is O(n) per partition (with O(1) /
   * amortized-O(1) incremental aggregator updates).
   */
  private List<Object> processRangeOffsetWindow(List<Object[]> rows) {
    int numRows = rows.size();
    int[][] frameBounds = computeRangeFrameBounds(rows);
    int[] starts = frameBounds[0];
    int[] ends = frameBounds[1];

    List<Object> result = new ArrayList<>(numRows);
    // The aggregator currently holds the rows in [curLeft, curRight - 1]. Both pointers only move forward.
    int curLeft = 0;
    int curRight = 0;
    for (int i = 0; i < numRows; i++) {
      int start = starts[i];
      int end = ends[i];
      while (curRight <= end) {
        _windowValueAggregator.addValue(extractValueFromRow(rows.get(curRight)));
        curRight++;
      }
      // Advance past rows before 'start'. Only remove rows that were actually added (guards against empty-frame gaps).
      while (curLeft < start) {
        if (curLeft < curRight) {
          _windowValueAggregator.removeValue(extractValueFromRow(rows.get(curLeft)));
        }
        curLeft++;
      }
      // When the frame is empty (start > end) the aggregator holds no values and returns the identity (NULL for
      // SUM/AVG/MIN/MAX, 0 for COUNT), matching standard SQL semantics.
      result.add(_windowValueAggregator.getCurrentAggregatedValue());
    }
    return result;
  }

  /**
   * ROWS frame with a non-default EXCLUDE clause. Loads the base frame into the aggregator and removes / re-adds the
   * excluded values per row. Peer-group boundaries are precomputed once per partition.
   */
  private List<Object> processRowsWindowWithExclude(List<Object[]> rows) {
    int numRows = rows.size();
    WindowNode.WindowExclusion exclude = _windowFrame.getExclude();
    int[] peerStart = null;
    int[] peerEnd = null;
    if (needsPeerBoundaries(exclude)) {
      peerStart = new int[numRows];
      peerEnd = new int[numRows];
      computePeerBoundaries(rows, peerStart, peerEnd);
    }

    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    for (int i = Math.max(0, lowerBound); i <= upperBound; i++) {
      _windowValueAggregator.addValue(extractValueFromRow(rows.get(i)));
    }

    List<Object> result = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        return result;
      }
      int frameStart = Math.max(0, lowerBound);
      int frameEnd = upperBound;
      int pStart = peerStart != null ? peerStart[i] : i;
      int pEnd = peerEnd != null ? peerEnd[i] : i;

      applyExclude(rows, i, frameStart, frameEnd, pStart, pEnd, exclude, true);
      result.add(_windowValueAggregator.getCurrentAggregatedValue());
      applyExclude(rows, i, frameStart, frameEnd, pStart, pEnd, exclude, false);

      if (lowerBound >= 0) {
        _windowValueAggregator.removeValue(extractValueFromRow(rows.get(lowerBound)));
      }
      lowerBound++;
      if (upperBound < numRows - 1) {
        upperBound++;
        if (upperBound >= 0) {
          _windowValueAggregator.addValue(extractValueFromRow(rows.get(upperBound)));
        }
      }
    }
    return result;
  }

  /**
   * RANGE frame with a non-default EXCLUDE clause. The frame for each row is determined by its peer group; we maintain
   * the aggregator state corresponding to the base frame and apply per-row EXCLUDE corrections.
   */
  private List<Object> processRangeWindowWithExclude(List<Object[]> rows) {
    int numRows = rows.size();
    int[] peerStart = new int[numRows];
    int[] peerEnd = new int[numRows];
    computePeerBoundaries(rows, peerStart, peerEnd);

    boolean lowerCurrentRow = _windowFrame.isLowerBoundCurrentRow();
    boolean upperCurrentRow = _windowFrame.isUpperBoundCurrentRow();
    WindowNode.WindowExclusion exclude = _windowFrame.getExclude();
    List<Object> result = new ArrayList<>(numRows);

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      // Frame = whole partition for every row
      for (Object[] row : rows) {
        _windowValueAggregator.addValue(extractValueFromRow(row));
      }
      for (int i = 0; i < numRows; i++) {
        applyExclude(rows, i, 0, numRows - 1, peerStart[i], peerEnd[i], exclude, true);
        result.add(_windowValueAggregator.getCurrentAggregatedValue());
        applyExclude(rows, i, 0, numRows - 1, peerStart[i], peerEnd[i], exclude, false);
      }
      return result;
    }

    if (_windowFrame.isUnboundedPreceding() && upperCurrentRow) {
      // Frame for row i = [0, peerEnd[i]]; aggregator is built peer-group by peer-group
      int loaded = 0;
      int i = 0;
      while (i < numRows) {
        int end = peerEnd[i];
        for (int j = loaded; j <= end; j++) {
          _windowValueAggregator.addValue(extractValueFromRow(rows.get(j)));
        }
        loaded = end + 1;
        while (i <= end) {
          applyExclude(rows, i, 0, end, peerStart[i], peerEnd[i], exclude, true);
          result.add(_windowValueAggregator.getCurrentAggregatedValue());
          applyExclude(rows, i, 0, end, peerStart[i], peerEnd[i], exclude, false);
          i++;
        }
      }
      return result;
    }

    if (lowerCurrentRow && _windowFrame.isUnboundedFollowing()) {
      // Frame for row i = [peerStart[i], numRows-1]; build up the aggregator peer group by peer group, from the
      // rightmost peer toward the leftmost. After adding peer g, the aggregator contains [peerStart_g, numRows-1].
      Object[] perRow = new Object[numRows];
      int i = numRows - 1;
      while (i >= 0) {
        int start = peerStart[i];
        for (int j = start; j <= i; j++) {
          _windowValueAggregator.addValue(extractValueFromRow(rows.get(j)));
        }
        for (int j = start; j <= i; j++) {
          applyExclude(rows, j, start, numRows - 1, peerStart[j], peerEnd[j], exclude, true);
          perRow[j] = _windowValueAggregator.getCurrentAggregatedValue();
          applyExclude(rows, j, start, numRows - 1, peerStart[j], peerEnd[j], exclude, false);
        }
        i = start - 1;
      }
      Collections.addAll(result, perRow);
      return result;
    }

    if (lowerCurrentRow && upperCurrentRow) {
      // Frame for row i = peer group of i; load each peer group separately
      int i = 0;
      while (i < numRows) {
        int start = peerStart[i];
        int end = peerEnd[i];
        for (int j = start; j <= end; j++) {
          _windowValueAggregator.addValue(extractValueFromRow(rows.get(j)));
        }
        while (i <= end) {
          applyExclude(rows, i, start, end, start, end, exclude, true);
          result.add(_windowValueAggregator.getCurrentAggregatedValue());
          applyExclude(rows, i, start, end, start, end, exclude, false);
          i++;
        }
        for (int j = start; j <= end; j++) {
          _windowValueAggregator.removeValue(extractValueFromRow(rows.get(j)));
        }
      }
      return result;
    }

    // Value-based RANGE offset frame (e.g. RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) with a non-default EXCLUDE.
    return processRangeOffsetWindowWithExclude(rows);
  }

  /**
   * Value-based RANGE offset frame with a non-default EXCLUDE clause. Slides the aggregator over the base frame bounds
   * computed from the ORDER BY value (like {@link #processRangeOffsetWindow}) and applies the per-row EXCLUDE
   * corrections on top of that base frame. The aggregator supports arbitrary removal (MIN / MAX use a sorted multiset)
   * since EXCLUDE re-adds previously removed values.
   */
  private List<Object> processRangeOffsetWindowWithExclude(List<Object[]> rows) {
    int numRows = rows.size();
    int[][] frameBounds = computeRangeFrameBounds(rows);
    int[] starts = frameBounds[0];
    int[] ends = frameBounds[1];
    WindowNode.WindowExclusion exclude = _windowFrame.getExclude();
    int[] peerStart = null;
    int[] peerEnd = null;
    if (needsPeerBoundaries(exclude)) {
      peerStart = new int[numRows];
      peerEnd = new int[numRows];
      computePeerBoundaries(rows, peerStart, peerEnd);
    }

    List<Object> result = new ArrayList<>(numRows);
    // The aggregator holds the base frame [curLeft, curRight - 1]; both pointers only move forward.
    int curLeft = 0;
    int curRight = 0;
    for (int i = 0; i < numRows; i++) {
      int start = starts[i];
      int end = ends[i];
      while (curRight <= end) {
        _windowValueAggregator.addValue(extractValueFromRow(rows.get(curRight)));
        curRight++;
      }
      while (curLeft < start) {
        if (curLeft < curRight) {
          _windowValueAggregator.removeValue(extractValueFromRow(rows.get(curLeft)));
        }
        curLeft++;
      }
      int pStart = peerStart != null ? peerStart[i] : i;
      int pEnd = peerEnd != null ? peerEnd[i] : i;
      // Remove the EXCLUDE set from the base frame [start, end], read the result, then re-add it for the next row.
      applyExclude(rows, i, start, end, pStart, pEnd, exclude, true);
      result.add(_windowValueAggregator.getCurrentAggregatedValue());
      applyExclude(rows, i, start, end, pStart, pEnd, exclude, false);
    }
    return result;
  }

  /**
   * Removes (when {@code remove} is true) or re-adds (otherwise) the rows in the EXCLUDE set, restricted to the base
   * frame {@code [frameStart, frameEnd]}. The exclude set is derived from the current row {@code i} and its peer group
   * {@code [pStart, pEnd]} as defined by SQL's EXCLUDE clause.
   */
  private void applyExclude(List<Object[]> rows, int i, int frameStart, int frameEnd, int pStart, int pEnd,
      WindowNode.WindowExclusion exclude, boolean remove) {
    switch (exclude) {
      case CURRENT_ROW:
        if (i >= frameStart && i <= frameEnd) {
          Object value = extractValueFromRow(rows.get(i));
          if (remove) {
            _windowValueAggregator.removeValue(value);
          } else {
            _windowValueAggregator.addValue(value);
          }
        }
        break;
      case GROUP: {
        int from = Math.max(pStart, frameStart);
        int to = Math.min(pEnd, frameEnd);
        for (int j = from; j <= to; j++) {
          Object value = extractValueFromRow(rows.get(j));
          if (remove) {
            _windowValueAggregator.removeValue(value);
          } else {
            _windowValueAggregator.addValue(value);
          }
        }
        break;
      }
      case TIES: {
        int from = Math.max(pStart, frameStart);
        int to = Math.min(pEnd, frameEnd);
        for (int j = from; j <= to; j++) {
          if (j == i) {
            continue;
          }
          Object value = extractValueFromRow(rows.get(j));
          if (remove) {
            _windowValueAggregator.removeValue(value);
          } else {
            _windowValueAggregator.addValue(value);
          }
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported WindowExclusion: " + exclude);
    }
  }
}
