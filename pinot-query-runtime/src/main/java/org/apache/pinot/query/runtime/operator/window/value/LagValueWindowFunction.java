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
 * Window function that returns the value of a column from a preceding row within the partition. Supports an optional
 * offset (default 1), an optional default value for when no row exists at that offset, and IGNORE NULLS mode which
 * skips null values when scanning backward. Custom window frames are not allowed (enforced by Calcite).
 */
public class LagValueWindowFunction extends OffsetValueWindowFunction {

  public LagValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    if (_ignoreNulls && _offset > 0) {
      // Iterate front-to-back so the sliding window holds the preceding non-null values.
      return processRowsIgnoreNulls(rows, true);
    }
    int numRows = rows.size();
    Object[] result = new Object[numRows];
    if (_defaultValue != null) {
      // We only fill up to the minimum of _offset and numRows to handle the case
      // where the offset is larger than the result size.
      int fillTo = Math.min(_offset, numRows);
      Arrays.fill(result, 0, fillTo, _defaultValue);
    }
    for (int i = _offset; i < numRows; i++) {
      result[i] = extractValueFromRow(rows.get(i - _offset));
    }
    return Arrays.asList(result);
  }
}
