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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


public class FirstValueWindowFunction extends ValueWindowFunction {

  public FirstValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
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
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.getUpperBound() >= 0) {
      return fillAllWithValue(rows, extractValueFromRow(rows.get(0)));
    }

    int numRows = rows.size();
    List<Object> result = new ArrayList<>(numRows);

    // lowerBound is guaranteed to be less than or equal to upperBound here (but both can be -ve / +ve)
    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        // Fill remaining rows with null
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        break;
      }

      if (upperBound >= 0) {
        result.add(extractValueFromRow(rows.get(Math.max(0, lowerBound))));
      } else {
        result.add(null);
      }

      lowerBound++;
      upperBound = Math.min(upperBound + 1, numRows - 1);
    }

    return result;
  }

  private List<Object> processRowsWindowIgnoreNulls(List<Object[]> rows) {
    int numRows = rows.size();
    int lowerBound = _windowFrame.getLowerBound();
    int upperBound = Math.min(_windowFrame.getUpperBound(), numRows - 1);

    int indexOfFirstNonNullValue = -1;
    // Find first non-null value in the first window
    if (lowerBound < numRows && upperBound >= 0) {
      for (int i = Math.max(lowerBound, 0); i <= Math.min(upperBound, numRows - 1); i++) {
        Object value = extractValueFromRow(rows.get(i));
        if (value != null) {
          indexOfFirstNonNullValue = i;
          break;
        }
      }
    }

    List<Object> result = new ArrayList<>();

    for (int i = 0; i < numRows; i++) {
      if (lowerBound >= numRows) {
        // Fill the remaining rows with null
        for (int j = i; j < numRows; j++) {
          result.add(null);
        }
        break;
      }

      if (indexOfFirstNonNullValue != -1) {
        result.add(extractValueFromRow(rows.get(indexOfFirstNonNullValue)));
      } else {
        result.add(null);
      }

      // Slide the window forward by one row
      if (indexOfFirstNonNullValue == lowerBound) {
        indexOfFirstNonNullValue = -1;
        // Find first non-null value for the next window
        for (int j = Math.max(lowerBound + 1, 0); j <= Math.min(upperBound + 1, numRows - 1); j++) {
          Object value = extractValueFromRow(rows.get(j));
          if (value != null) {
            indexOfFirstNonNullValue = j;
            break;
          }
        }
      }
      lowerBound++;

      if (upperBound < numRows - 1) {
        upperBound++;
        if (indexOfFirstNonNullValue == -1 && upperBound >= 0) {
          Object value = extractValueFromRow(rows.get(upperBound));
          if (value != null) {
            indexOfFirstNonNullValue = upperBound;
          }
        }
      }
    }

    return result;
  }

  private List<Object> processRangeWindow(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding()) {
      return fillAllWithValue(rows, extractValueFromRow(rows.get(0)));
    }

    // The lower bound has to be CURRENT ROW since we don't support RANGE windows with offset value
    Preconditions.checkState(_windowFrame.isLowerBoundCurrentRow(),
        "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");

    int numRows = rows.size();
    List<Object> result = new ArrayList<>(numRows);

    // The window frame here is either RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING or RANGE BETWEEN CURRENT ROW
    // AND CURRENT ROW. In both cases, the result for each row is the value of the first row in the partition with the
    // same order key as the current row.
    Map<Key, Object> firstValueForKey = new HashMap<>();
    for (Object[] row : rows) {
      Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);

      // Two map lookups used intentionally to differentiate between explicit null values versus missing keys
      if (firstValueForKey.containsKey(orderKey)) {
        result.add(firstValueForKey.get(orderKey));
      } else {
        Object value = extractValueFromRow(row);
        result.add(value);
        firstValueForKey.put(orderKey, value);
      }
    }

    return result;
  }

  private List<Object> processRangeWindowIgnoreNulls(List<Object[]> rows) {
    int numRows = rows.size();

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUnboundedFollowing()) {
      // Find the first non-null value and fill it in all rows
      for (int i = 0; i < numRows; i++) {
        Object[] row = rows.get(i);
        Object value = extractValueFromRow(row);
        if (value != null) {
          return fillAllWithValue(rows, value);
        }
      }
      // There's no non-null value
      return Collections.nCopies(numRows, null);
    }

    if (_windowFrame.isUnboundedPreceding() && _windowFrame.isUpperBoundCurrentRow()) {
      // Find the first non-null value and fill it in all rows starting from the first row of the peer group of the row
      // with the first non-null value
      int firstNonNullValueIndex = -1;
      Key firstNonNullValueKey = null;
      for (int i = 0; i < numRows; i++) {
        Object[] row = rows.get(i);
        Object value = extractValueFromRow(row);
        if (value != null) {
          firstNonNullValueIndex = i;
          firstNonNullValueKey = AggregationUtils.extractRowKey(row, _orderKeys);
          break;
        }
      }

      // No non-null values
      if (firstNonNullValueIndex == -1) {
        return Collections.nCopies(numRows, null);
      }

      List<Object> result = new ArrayList<>(numRows);
      // Find the start of the peer group of the row with the first non-null value
      int i;
      for (i = 0; i < numRows; i++) {
        Object[] row = rows.get(i);
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        if (orderKey.equals(firstNonNullValueKey)) {
          break;
        } else {
          result.add(null);
        }
      }

      Object firstNonNullValue = extractValueFromRow(rows.get(firstNonNullValueIndex));
      for (; i < numRows; i++) {
        result.add(firstNonNullValue);
      }

      return result;
    }

    if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUpperBoundCurrentRow()) {
      List<Object> result = new ArrayList<>(numRows);
      Map<Key, Object> firstValueForKey = new HashMap<>();

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object value = extractValueFromRow(row);

        if (value != null) {
          firstValueForKey.putIfAbsent(orderKey, value);
        }
      }

      for (Object[] row : rows) {
        result.add(firstValueForKey.get(AggregationUtils.extractRowKey(row, _orderKeys)));
      }

      return result;
    }

    if (_windowFrame.isLowerBoundCurrentRow() && _windowFrame.isUnboundedFollowing()) {
      List<Object> result = new ArrayList<>(numRows);
      Map<Key, Object> firstValueForKey = new HashMap<>();

      for (Object[] row : rows) {
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object value = extractValueFromRow(row);

        if (value != null) {
          firstValueForKey.putIfAbsent(orderKey, value);
        }
      }

      // Do a reverse iteration to get the first non-null value for each group. The first non-null value could either
      // belong to the current group or any of the next groups if all the values in the current group are null.
      Object prevNonNullValue = null;
      for (int i = numRows - 1; i >= 0; i--) {
        Object[] row = rows.get(i);
        Key orderKey = AggregationUtils.extractRowKey(row, _orderKeys);
        Object value = firstValueForKey.get(orderKey);

        if (value != null) {
          prevNonNullValue = value;
        }

        result.add(prevNonNullValue);
      }

      Collections.reverse(result);
      return result;
    }

    throw new IllegalStateException("RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
  }
}
