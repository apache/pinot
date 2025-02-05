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
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;
import org.apache.pinot.spi.exception.QException;


public class AggregateWindowFunction extends WindowFunction {
  private final WindowValueAggregator<Object> _windowValueAggregator;
  private final String _functionName;

  public AggregateWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
    _functionName = aggCall.getFunctionName();
    _windowValueAggregator = WindowValueAggregatorFactory.getWindowValueAggregator(_functionName, _dataType,
        windowFrame.isRowType() && !(_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()));
  }

  @Override
  public final List<Object> processRows(List<Object[]> rows) {
    _windowValueAggregator.clear();
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
    try {
      for (Object[] row : rows) {
        _windowValueAggregator.addValue(extractValueFromRow(row));
      }
    } catch (ClassCastException e) {
      throw new QException(QException.SQL_RUNTIME_ERROR_CODE,
          "Failed to cast value as expected in " + _functionName, e);
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
      throw new IllegalStateException("RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
    }
  }
}
