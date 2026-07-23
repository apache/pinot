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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


/**
 * Base class for the offset-based value window functions {@code LAG} and {@code LEAD}. Both return the value of the
 * function's argument from a row at a fixed offset (default 1) before ({@code LAG}) or after ({@code LEAD}) the current
 * row within the partition, support an optional default value emitted when no such row exists, and support the
 * {@code IGNORE NULLS} option which skips null values when counting the offset.
 *
 * <p>Custom window frames are not allowed for these functions (enforced by Calcite).
 *
 * <p>Instances are not thread-safe. The multi-stage window operator creates one instance per operator and invokes it
 * from at most one thread at a time (never concurrently), so no synchronization is required.
 */
public abstract class OffsetValueWindowFunction extends ValueWindowFunction {
  protected final int _offset;
  @Nullable
  protected final Object _defaultValue;

  public OffsetValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
    String functionName = aggCall.getFunctionName();
    List<RexExpression> operands = aggCall.getFunctionOperands();
    int numOperands = operands.size();
    Preconditions.checkArgument(numOperands > 0, "%s function requires at least one operand", functionName);
    int offset = 1;
    if (numOperands > 1) {
      RexExpression secondOperand = operands.get(1);
      Preconditions.checkArgument(secondOperand instanceof RexExpression.Literal,
          "Second operand (offset) of %s function must be a literal", functionName);
      Object offsetValue = ((RexExpression.Literal) secondOperand).getValue();
      if (offsetValue instanceof Number) {
        offset = ((Number) offsetValue).intValue();
      }
    }
    Object defaultValue = null;
    if (numOperands == 3) {
      RexExpression thirdOperand = operands.get(2);
      Preconditions.checkArgument(thirdOperand instanceof RexExpression.Literal,
          "Third operand (default value) of %s function must be a literal", functionName);
      RexExpression.Literal defaultValueLiteral = (RexExpression.Literal) thirdOperand;
      defaultValue = defaultValueLiteral.getValue();
      if (defaultValue != null) {
        DataSchema.ColumnDataType srcDataType = defaultValueLiteral.getDataType();
        // Coerce the default value to the type of the function's value argument (getDataType()) so it matches the type
        // of the values returned for the non-default rows. Note this is the argument's type, not necessarily the first
        // input column's type.
        DataSchema.ColumnDataType destDataType = getDataType();
        if (srcDataType != destDataType) {
          defaultValue = destDataType.toPinotDataType().convert(defaultValue, srcDataType.toPinotDataType());
        }
      }
    }
    _offset = offset;
    _defaultValue = defaultValue;
  }

  /**
   * Shared {@code IGNORE NULLS} implementation for {@code LAG}/{@code LEAD}. Walks the partition in the given direction
   * maintaining a bounded sliding window of the {@code _offset} most-recently-seen non-null values; when the window is
   * full, its oldest element ({@code peekFirst()}) is the offset-th non-null value in the scan direction relative to
   * the current row. Until {@code _offset} non-null values have been seen, the default value is emitted.
   *
   * <p>Runs in O(numRows) time and O(min(_offset, numRows)) memory. Requires {@code _offset >= 1}; the degenerate
   * {@code _offset <= 0} case (which has no null-skipping semantics) must be routed through the RESPECT NULLS path by
   * the caller.
   *
   * @param rows the rows of a single partition, in partition/collation order
   * @param iterateForward {@code true} to iterate front-to-back (used by {@code LAG}, which looks at preceding rows);
   *                       {@code false} to iterate back-to-front (used by {@code LEAD}, which looks at following rows)
   */
  protected List<Object> processRowsIgnoreNulls(List<Object[]> rows, boolean iterateForward) {
    int numRows = rows.size();
    Object[] result = new Object[numRows];
    // Cap the initial capacity at the number of rows so a pathologically large offset literal can't force a huge eager
    // allocation (the deque never holds more than min(_offset, numRows) elements). The +1 leaves room for the transient
    // (_offset + 1)-th element that is added before being polled, avoiding a resize in the common case.
    ArrayDeque<Object> window = new ArrayDeque<>(Math.min(_offset, numRows) + 1);
    for (int k = 0; k < numRows; k++) {
      int i = iterateForward ? k : numRows - 1 - k;
      result[i] = window.size() == _offset ? window.peekFirst() : _defaultValue;
      Object value = extractValueFromRow(rows.get(i));
      if (value != null) {
        window.addLast(value);
        if (window.size() > _offset) {
          window.pollFirst();
        }
      }
    }
    return Arrays.asList(result);
  }
}
