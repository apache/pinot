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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.BooleanUtils;


/**
 * Utility class to perform accumulation over a collection of rows. It provides utils for the following:
 * (1) method to deal with aggregation key and
 * (2) method to merge a row into an existing accumulator
 *
 * <p>Accumulation is used by {@code WindowAggregateOperator} and {@code AggregateOperator}.
 */
public class AggregationUtils {
  private AggregationUtils() {
  }

  public static Key extractRowKey(Object[] row, int[] indices) {
    int numKeys = indices.length;
    Object[] values = new Object[numKeys];
    for (int i = 0; i < numKeys; i++) {
      values[i] = row[indices[i]];
    }
    return new Key(values);
  }

  public static Key extractEmptyKey() {
    return new Key(new Object[0]);
  }

  // TODO: Use the correct type for SUM/MIN/MAX instead of always using double

  @Nullable
  private static Object mergeSum(@Nullable Object agg, @Nullable Object value) {
    if (agg == null) {
      return value;
    }
    if (value == null) {
      return agg;
    }
    return ((Number) agg).doubleValue() + ((Number) value).doubleValue();
  }

  @Nullable
  private static Object mergeMin(@Nullable Object agg, @Nullable Object value) {
    if (agg == null) {
      return value;
    }
    if (value == null) {
      return agg;
    }
    return Math.min(((Number) agg).doubleValue(), ((Number) value).doubleValue());
  }

  @Nullable
  private static Object mergeMax(@Nullable Object agg, @Nullable Object value) {
    if (agg == null) {
      return value;
    }
    if (value == null) {
      return agg;
    }
    return Math.max(((Number) agg).doubleValue(), ((Number) value).doubleValue());
  }

  /**
   * NOTE: Arguments are in internal type. See {@link ColumnDataType#toInternal} for more details.
   *
   * <p>Null handling:
   * <ul>
   *   <li>Null & Null/True -> Null</li>
   *   <li>Null & False -> False</li>
   * </ul>
   */
  @Nullable
  private static Object mergeBoolAnd(@Nullable Object agg, @Nullable Object value) {
    // Return FALSE when any argument is FALSE
    if (BooleanUtils.isFalseInternalValue(agg) || BooleanUtils.isFalseInternalValue(value)) {
      return BooleanUtils.INTERNAL_FALSE;
    }
    // Otherwise, return NULL when any argument is NULL
    if (agg == null || value == null) {
      return null;
    }
    return BooleanUtils.INTERNAL_TRUE;
  }

  /**
   * NOTE: Arguments are in internal type. See {@link ColumnDataType#toInternal} for more details.
   *
   * <p>Null handling:
   * <ul>
   *   <li>Null | Null/False -> Null</li>
   *   <li>Null | True -> True</li>
   * </ul>
   */
  @Nullable
  private static Object mergeBoolOr(@Nullable Object agg, @Nullable Object value) {
    // Return TRUE when any argument is TRUE
    if (BooleanUtils.isTrueInternalValue(agg) || BooleanUtils.isTrueInternalValue(value)) {
      return BooleanUtils.INTERNAL_TRUE;
    }
    // Otherwise, return NULL when any argument is NULL
    if (agg == null || value == null) {
      return null;
    }
    return BooleanUtils.INTERNAL_FALSE;
  }

  private static class MergeCounts implements AggregationUtils.Merger {

    @Override
    public Long init(@Nullable Object value, ColumnDataType dataType) {
      return value == null ? 0L : 1L;
    }

    @Override
    public Long merge(Object agg, @Nullable Object value) {
      return value == null ? (long) agg : (long) agg + 1;
    }
  }

  public interface Merger {

    /**
     * Initializes the merger based on the column data type and first value.
     */
    @Nullable
    default Object init(@Nullable Object value, ColumnDataType dataType) {
      return value;
    }

    /**
     * Merges the existing aggregate (the result of {@link #init(Object, ColumnDataType)}) with
     * the new value coming in (which may be an aggregate in and of itself).
     */
    @Nullable
    Object merge(@Nullable Object agg, @Nullable Object value);
  }

  /**
   * Accumulator class which accumulates the aggregated results into the group sets if any
   */
  public static class Accumulator {
    //@formatter:off
    public static final Map<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> MERGERS = Map.of(
        "SUM", cdt -> AggregationUtils::mergeSum,
        // NOTE: Keep both 'SUM0' and '$SUM0' for backward compatibility where 'SUM0' is SqlKind and '$SUM0' is function
        //       name.
        "SUM0", cdt -> AggregationUtils::mergeSum,
        "$SUM0", cdt -> AggregationUtils::mergeSum,
        "MIN", cdt -> AggregationUtils::mergeMin,
        "MAX", cdt -> AggregationUtils::mergeMax,
        "COUNT", cdt -> new AggregationUtils.MergeCounts(),
        "BOOLAND", cdt -> AggregationUtils::mergeBoolAnd,
        "BOOLOR", cdt -> AggregationUtils::mergeBoolOr
    );
    //@formatter:on

    protected final int _inputRef;
    protected final Object _literal;
    protected final Map<Key, Object> _results = new HashMap<>();
    protected final ColumnDataType _dataType;

    public Map<Key, Object> getResults() {
      return _results;
    }

    public ColumnDataType getDataType() {
      return _dataType;
    }

    public Accumulator(RexExpression.FunctionCall aggCall, DataSchema inputSchema) {
      // agg function operand should either be a InputRef or a Literal
      RexExpression operand = toAggregationFunctionOperand(aggCall);
      if (operand instanceof RexExpression.InputRef) {
        _inputRef = ((RexExpression.InputRef) operand).getIndex();
        _literal = null;
        _dataType = inputSchema.getColumnDataType(_inputRef);
      } else {
        _inputRef = -1;
        RexExpression.Literal literal = (RexExpression.Literal) operand;
        _literal = literal.getValue();
        _dataType = literal.getDataType();
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall aggCall) {
      List<RexExpression> functionOperands = aggCall.getFunctionOperands();
      int numOperands = functionOperands.size();
      return numOperands == 0 ? new RexExpression.Literal(ColumnDataType.INT, 1) : functionOperands.get(0);
    }
  }
}
