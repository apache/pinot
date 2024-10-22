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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.aggregate.BoolAndValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.BoolOrValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.CountWindowValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.MaxWindowValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.MinWindowValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.SumWindowValueAggregator;
import org.apache.pinot.query.runtime.operator.window.aggregate.WindowValueAggregator;


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

  /**
   * Accumulator class which accumulates the aggregated results into the group sets if any
   */
  public static class Accumulator {
    //@formatter:off
    // TODO: Add type specific aggregator implementations
    public static final Map<String, Function<DataSchema.ColumnDataType, WindowValueAggregator<Object>>> AGGREGATORS =
        // NOTE: Keep both 'SUM0' and '$SUM0' for backward compatibility where 'SUM0' is SqlKind and '$SUM0' is function
        //       name.
        Map.of(
            "SUM", cdt -> new SumWindowValueAggregator(),
            "SUM0", cdt -> new SumWindowValueAggregator(),
            "$SUM0", cdt -> new SumWindowValueAggregator(),
            "MIN", cdt -> new MinWindowValueAggregator(),
            "MAX", cdt -> new MaxWindowValueAggregator(),
            "COUNT", cdt -> new CountWindowValueAggregator(),
            "BOOLAND", cdt -> new BoolAndValueAggregator(),
            "BOOLOR", cdt -> new BoolOrValueAggregator()
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
