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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


/**
 * Window function that returns the value of a column from a subsequent row within the partition. Supports an optional
 * offset (default 1), an optional default value for when no row exists at that offset, and IGNORE NULLS mode which
 * skips null values when scanning forward. Custom window frames are not allowed (enforced by Calcite).
 */
public class LeadValueWindowFunction extends OffsetValueWindowFunction {

  public LeadValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    if (_ignoreNulls && _offset > 0) {
      // Iterate back-to-front so the sliding window holds the upcoming non-null values.
      return processRowsIgnoreNulls(rows, false);
    }
    int numRows = rows.size();
    Object[] result = new Object[numRows];
    for (int i = 0; i < numRows - _offset; i++) {
      result[i] = extractValueFromRow(rows.get(i + _offset));
    }
    if (_defaultValue != null) {
      // If an offset is provided beyond the number of rows, fill all with default value
      // only down to 0.
      int fillFrom = Math.max(numRows - _offset, 0);
      Arrays.fill(result, fillFrom, numRows, _defaultValue);
    }
    return Arrays.asList(result);
  }
}
