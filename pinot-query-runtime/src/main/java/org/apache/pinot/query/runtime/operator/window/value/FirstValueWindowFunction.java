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
import java.util.Arrays;
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
      return processRowsWindow(rows);
    } else {
      return processRangeWindow(rows);
    }
  }

  private List<Object> processRowsWindow(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding() && _windowFrame.getUpperBound() >= 0) {
      return processUnboundedPreceding(rows);
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

  private List<Object> processRangeWindow(List<Object[]> rows) {
    if (_windowFrame.isUnboundedPreceding()) {
      return processUnboundedPreceding(rows);
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

  private List<Object> processUnboundedPreceding(List<Object[]> rows) {
    int numRows = rows.size();
    assert numRows > 0;
    Object value = extractValueFromRow(rows.get(0));
    Object[] result = new Object[numRows];
    Arrays.fill(result, value);
    return Arrays.asList(result);
  }
}
