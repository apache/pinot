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
package org.apache.pinot.query.runtime.operator.window.value;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.collections.DualValueList;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


public class LastValueWindowFunction extends ValueWindowFunction {

  public LastValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    if (_windowFrame.isRowType()) {
      if (_ignoreNulls) {
        return processRowsWindowIgnoreNulls(rows);
      } else {
        return processRowsWindow(rows);
      }
    } else {
      if (_ignoreNulls) {
        return processRangeWindowIgnoreNulls(rows);
      } else {
        return processRangeWindow(rows);
      }
    }
  }

  private List<Object> processRowsWindow(List<Object[]> rows) {
    int numRows = rows.size();
    if (_windowFrame.isUnboundedFollowing() && _windowFrame.getLowerBound() <= 0) {
      return Collections.nCopies(numRows, extractValueFromRow(rows.get(numRows - 1)));
    }

    List<Object> result = new ArrayList<>(numRows);

    // lowerBound is guaranteed to be less than or equal to upperBound here (but both can be -ve / +ve)
    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        // Fill the remaining rows with null since all subsequent windows will be out of bounds
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        break;
      }

      if (upperBound >= 0) {
        result.add(extractValueFromRow(rows.get(upperBound)));
      } else {
        result.add(null);
      }

      lowerBound++;
      upperBound = Math.min(upperBound + 1, numRows - 1);
    }

    return result;
  }

  private List<Object> processRowsWindowIgnoreNulls(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedWindowIgnoreNulls(rows);
    }

    int numRows = rows.size();
    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    // Find last non-null value in the first window
    int indexOfLastNonNullValue = indexOfLastNonNullValueInWindow(rows, Math.max(0, lowerBound), upperBound);

    List<Object> result = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        // Fill the remaining rows with null since all subsequent windows will be out of bounds
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        break;
      }

      if (indexOfLastNonNullValue != -1) {
        result.add(extractValueFromRow(rows.get(indexOfLastNonNullValue)));
      } else {
        result.add(null);
      }

      // Slide the window forward by one row; check if indexOfLastNonNullValue is the lower bound which will not be in
      // the next window. If so, update it to -1 since the window no longer has any non-null values.
      if (indexOfLastNonNullValue == lowerBound) {
        indexOfLastNonNullValue = -1;
      }
      lowerBound++;

      // After the lower bound is updated, we also update the upper bound for the next window. The upper bound is only
      // incremented if we're not already at the row boundary. If the upper bound is incremented, we also need to update
      // indexOfLastNonNullValue to the new upper bound if it contains a non-null value.
      if (upperBound < numRows - 1) {
        upperBound++;
        if (upperBound >= 0) {
          Object value = extractValueFromRow(rows.get(upperBound));
          if (value != null) {
            indexOfLastNonNullValue = upperBound;
          }
        }
      }
    }

    return result;
  }

  private List<Object> processRangeWindow(List<Object[]> rows) {
    int numRows = rows.size();
    if (_windowFrame.isUnboundedFollowing()) {
      return Collections.nCopies(numRows, extractValueFromRow(rows.get(numRows - 1)));
    }

    // The upper bound has to be CURRENT ROW here since we don't support RANGE windows with offset value
    Preconditions.checkState(_windowFrame.isUpperBoundCurrentRow(),
        "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");

    List<Object> result = new ArrayList<>(numRows);
    Map<Key, Object> lastValueForKey = new HashMap<>();

    // The window frame here is either RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW or RANGE BETWEEN CURRENT ROW
    // AND CURRENT ROW. In both cases, the result for each row is the value of the last row in the partition with the
    // same order key as the current row.
    for (int i = numRows - 1; i >= 0; i--) {
      Object[] row = rows.get(i);
      Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);

      // Two map lookups used intentionally to differentiate between explicit null values versus missing keys
      if (lastValueForKey.containsKey(orderKey)) {
        result.add(lastValueForKey.get(orderKey));
      } else {
        Object value = extractValueFromRow(row);
        result.add(value);
        lastValueForKey.put(orderKey, value);
      }
    }

    Collections.reverse(result);
    return result;
  }

  private List<Object> processRangeWindowIgnoreNulls(List<Object[]> rows) {
    int numRows = rows.size();

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      return processUnboundedWindowIgnoreNulls(rows);
    }

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUpperBoundCurrentRow()) {
      List<Object> result = new ArrayList<>(numRows);
      Map<Key, Object> lastValueForKey = new HashMap<>();
      Object lastNonNullValue = null;

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object value = extractValueFromRow(row);

        if (value != null) {
          lastValueForKey.put(orderKey, value);
          lastNonNullValue = value;
        } else {
          lastValueForKey.putIfAbsent(orderKey, lastNonNullValue);
        }
      }

      for (Object[] row : rows) {
        result.add(lastValueForKey.get(AggregationUtils.extractRowKey(row, _orderKeys)));
      }

      return result;
    }

    if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUpperBoundCurrentRow()) {
      List<Object> result = new ArrayList<>(numRows);
      Map<Key, Object> lastValueForKey = new HashMap<>();

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object value = extractValueFromRow(row);

        if (value != null) {
          lastValueForKey.put(orderKey, value);
        }
      }

      for (Object[] row : rows) {
        result.add(lastValueForKey.get(AggregationUtils.extractRowKey(row, _orderKeys)));
      }

      return result;
    }

    if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUnboundedFollowing()) {
      // Get last non-null value and fill it in all rows from the first row till the last row of the peer group of the
      // row with the non-null value
      int indexOfLastNonNullValue = indexOfLastNonNullValueInWindow(rows, 0, numRows - 1);
      Key lastNonNullValueKey;

      // No non-null values
      if (indexOfLastNonNullValue == -1) {
        return Collections.nCopies(numRows, null);
      } else {
        lastNonNullValueKey = AggregationUtils.extractRowKey(rows.get(indexOfLastNonNullValue), _orderKeys);
      }

      // Find the end of the peer group of the last row with the non-null value
      int fillBoundary;
      for (fillBoundary = indexOfLastNonNullValue + 1; fillBoundary < numRows; fillBoundary++) {
        if (!AggregationUtils.extractRowKey(rows.get(fillBoundary), _orderKeys).equals(lastNonNullValueKey)) {
          break;
        }
      }

      Object lastNonNullValue = extractValueFromRow(rows.get(indexOfLastNonNullValue));
      return new DualValueList<>(lastNonNullValue, fillBoundary, null, numRows - fillBoundary);
    }

    throw new IllegalStateException("RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
  }

  private List<Object> processUnboundedWindowIgnoreNulls(List<Object[]> rows) {
    int numRows = rows.size();
    // Find the last non-null value and fill it in all rows
    int indexOfLastNonNullValue = indexOfLastNonNullValueInWindow(rows, 0, numRows - 1);
    if (indexOfLastNonNullValue == -1) {
      // There's no non-null value
      return Collections.nCopies(numRows, null);
    } else {
      return Collections.nCopies(numRows, extractValueFromRow(rows.get(indexOfLastNonNullValue)));
    }
  }

  /**
   * Both lowerBound and upperBound should be valid values for the given row set. The returned value is -1 if there is
   * no non-null value in the window.
   */
  private int indexOfLastNonNullValueInWindow(List<Object[]> rows, int lowerBound, int upperBound) {
    for (int i = upperBound; i >= lowerBound; i--) {
      Object value = extractValueFromRow(rows.get(i));
      if (value != null) {
        return i;
      }
    }
    return -1;
  }
}
