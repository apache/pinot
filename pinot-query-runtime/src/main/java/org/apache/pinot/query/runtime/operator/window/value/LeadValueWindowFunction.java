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
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


/**
 * The LAG window function doesn't allow custom window frames (and this is enforced by Calcite).
 */
public class LeadValueWindowFunction extends ValueWindowFunction {

  private final int _offset;
  private final Object _defaultValue;

  public LeadValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
    int offset = 1;
    Object defaultValue = null;
    List<RexExpression> operands = aggCall.getFunctionOperands();
    int numOperands = operands.size();
    if (numOperands > 1) {
      RexExpression secondOperand = operands.get(1);
      Preconditions.checkArgument(secondOperand instanceof RexExpression.Literal,
          "Second operand (offset) of LEAD function must be a literal");
      Object offsetValue = ((RexExpression.Literal) secondOperand).getValue();
      if (offsetValue instanceof Number) {
        offset = ((Number) offsetValue).intValue();
      }
    }
    if (numOperands == 3) {
      RexExpression thirdOperand = operands.get(2);
      Preconditions.checkArgument(thirdOperand instanceof RexExpression.Literal,
          "Third operand (default value) of LEAD function must be a literal");
      RexExpression.Literal defaultValueLiteral = (RexExpression.Literal) thirdOperand;
      defaultValue = defaultValueLiteral.getValue();
      if (defaultValue != null) {
        DataSchema.ColumnDataType srcDataType = defaultValueLiteral.getDataType();
        DataSchema.ColumnDataType destDataType = inputSchema.getColumnDataType(0);
        if (srcDataType != destDataType) {
          // Convert the default value to the same data type as the input column
          // (e.g. convert INT to LONG, FLOAT to DOUBLE, etc.
          defaultValue = PinotDataType.getPinotDataTypeForExecution(destDataType)
              .convert(defaultValue, PinotDataType.getPinotDataTypeForExecution(srcDataType));
        }
      }
    }
    _offset = offset;
    _defaultValue = defaultValue;
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    int numRows = rows.size();
    Object[] result = new Object[numRows];
    for (int i = 0; i < numRows - _offset; i++) {
      result[i] = extractValueFromRow(rows.get(i + _offset));
    }
    if (_defaultValue != null) {
      Arrays.fill(result, numRows - _offset, numRows, _defaultValue);
    }
    return Arrays.asList(result);
  }
}
