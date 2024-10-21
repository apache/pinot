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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils.Merger;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;


public class AggregateWindowFunction extends WindowFunction {
  private final Merger _merger;

  public AggregateWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
    String functionName = aggCall.getFunctionName();
    Function<ColumnDataType, Merger> mergerCreator = AggregationUtils.Accumulator.MERGERS.get(functionName);
    Preconditions.checkArgument(mergerCreator != null, "Unsupported aggregate function: %s", functionName);
    _merger = mergerCreator.apply(_dataType);
  }

  @Override
  public final List<Object> processRows(List<Object[]> rows) {
    if (_windowFrame.isRowType()) {
      return processRowsWindow(rows);
    } else {
      return processRangeWindow(rows);
    }
  }

  /**
   * Process windows where both ends are unbounded. Both ROWS and RANGE windows can be processed similarly.
   */
  private List<Object> processUnboundedPrecedingAndFollowingWindow(List<Object[]> rows) {
    // Process all rows at once
    Object mergedResult = null;
    for (Object[] row : rows) {
      mergedResult = getMergedResult(mergedResult, row);
    }
    return Collections.nCopies(rows.size(), mergedResult);
  }

  @Nullable
  private Object getMergedResult(Object currentResult, Object[] row) {
    Object value = _inputRef == -1 ? _literal : row[_inputRef];
    if (value == null) {
      return currentResult;
    }
    if (currentResult == null) {
      return _merger.init(value, _dataType);
    } else {
      return _merger.merge(currentResult, value);
    }
  }

  private List<Object> processRowsWindow(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedPrecedingAndFollowingWindow(rows);
    }

    if (_windowFrame.isUnboundedPreceding()) {
      int upperBound = _windowFrame.getUpperBound();
      List<Object> results = new ArrayList<>(rows.size());
      Object mergedResult = null;

      // Calculate first window result
      if (upperBound >= 0) {
        for (int i = 0; i <= upperBound; i++) {
          if (i < rows.size()) {
            mergedResult = getMergedResult(mergedResult, rows.get(i));
          } else {
            break;
          }
        }
      }

      for (int i = 0; i < rows.size(); i++) {
        results.add(mergedResult);

        // Update merged result for next row
        if (i + upperBound + 1 < rows.size() && i + upperBound + 1 >= 0) {
          mergedResult = getMergedResult(mergedResult, rows.get(i + upperBound + 1));
        }
      }

      return results;
    }

    if (_windowFrame.isUnboundedFollowing()) {
      int lowerBound = _windowFrame.getLowerBound();
      List<Object> results = new ArrayList<>(rows.size());
      Object mergedResult = null;

      // Calculate last window result
      if (lowerBound <= 0) {
        for (int i = rows.size() - 1; i >= rows.size() - 1 + lowerBound; i--) {
          if (i >= 0) {
            mergedResult = getMergedResult(mergedResult, rows.get(i));
          } else {
            break;
          }
        }
      }

      for (int i = rows.size() - 1; i >= 0; i--) {
        results.add(mergedResult);

        // Update merged result for next row
        if (i + lowerBound - 1 < rows.size() && i + lowerBound - 1 >= 0) {
          mergedResult = getMergedResult(mergedResult, rows.get(i + lowerBound - 1));
        }
      }

      Collections.reverse(results);
      return results;
    }

    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), rows.size() - 1);

    // TODO: Optimize this to avoid recomputing the merged result for each window from scratch. We can use a simple
    // sliding window algorithm for aggregations like SUM and COUNT. For MIN, MAX, etc. we'll need an additional
    // structure like a deque or priority queue to keep track of the minimum / maximum values in the sliding window.
    List<Object> results = new ArrayList<>(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      if (lowerBound >= rows.size()) {
        // Fill rest of the rows with null since all subsequent windows will be out of bounds
        for (int j = i; j < rows.size(); j++) {
          results.add(null);
        }
        break;
      }

      Object mergedResult = null;
      if (upperBound >= 0) {
        for (int j = Math.max(0, lowerBound); j <= upperBound; j++) {
          mergedResult = getMergedResult(mergedResult, rows.get(j));
        }
      }
      results.add(mergedResult);

      lowerBound++;
      upperBound = Math.min(upperBound + 1, rows.size() - 1);
    }
    return results;
  }

  private List<Object> processRangeWindow(List<Object[]> rows) {
    // We don't currently support RANGE windows with offset FOLLOWING / PRECEDING and this is validated during planning
    // so we can safely assume that the lower bound is either UNBOUNDED PRECEDING or CURRENT ROW and the upper bound
    // is either UNBOUNDED FOLLOWING or CURRENT ROW.

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedPrecedingAndFollowingWindow(rows);
    }

    KeyedResults orderByResult = new KeyedResults();
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUpperBoundCurrentRow()) {
      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Key previousOrderKeyIfPresent = orderByResult.getPreviousKey();
        Object currentRes =
            previousOrderKeyIfPresent == null ? null : orderByResult.getKeyedResults().get(previousOrderKeyIfPresent);
        Object value = _inputRef == -1 ? _literal : row[_inputRef];
        if (currentRes == null) {
          orderByResult.addResult(orderKey, _merger.init(value, _dataType));
        } else {
          orderByResult.addResult(orderKey, _merger.merge(currentRes, value));
        }
      }
    } else if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUnboundedFollowing()) {
      // Do a reverse iteration
      for (int i = rows.size() - 1; i >= 0; i--) {
        Object[] row = rows.get(i);
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Key previousOrderKeyIfPresent = orderByResult.getPreviousKey();
        Object currentRes =
            previousOrderKeyIfPresent == null ? null : orderByResult.getKeyedResults().get(previousOrderKeyIfPresent);
        Object value = _inputRef == -1 ? _literal : row[_inputRef];
        if (currentRes == null) {
          orderByResult.addResult(orderKey, _merger.init(value, _dataType));
        } else {
          orderByResult.addResult(orderKey, _merger.merge(currentRes, value));
        }
      }
    } else if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUpperBoundCurrentRow()) {
      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object currentRes = orderByResult.getKeyedResults().get(orderKey);
        Object value = _inputRef == -1 ? _literal : row[_inputRef];
        if (currentRes == null) {
          orderByResult.addResult(orderKey, _merger.init(value, _dataType));
        } else {
          orderByResult.addResult(orderKey, _merger.merge(currentRes, value));
        }
      }
    } else {
      throw new IllegalStateException("RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
    }

    List<Object> results = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
      Object value = orderByResult.getKeyedResults().get(orderKey);
      results.add(value);
    }
    return results;
  }

  // Used to maintain running results for each key. Note that the key here is not the partition key but the key
  // generated for a row based on the window's ORDER BY keys.
  private static class KeyedResults {
    final Map<Key, Object> _keyedResults;
    Key _previousKey;

    KeyedResults() {
      _keyedResults = new HashMap<>();
      _previousKey = null;
    }

    void addResult(Key key, Object value) {
      // We expect to get the rows in order based on the ORDER BY key so it is safe to blindly assign the
      // current key as the previous key
      _keyedResults.put(key, value);
      _previousKey = key;
    }

    Map<Key, Object> getKeyedResults() {
      return _keyedResults;
    }

    Key getPreviousKey() {
      return _previousKey;
    }
  }
}
