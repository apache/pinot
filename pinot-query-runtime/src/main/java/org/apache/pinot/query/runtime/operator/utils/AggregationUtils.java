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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Utility class to perform aggregations in the intermediate stage operators such as {@code AggregateOperator} and
 * {@code WindowAggregateOperator}
 */
public class AggregationUtils {

  private AggregationUtils() {
  }

  public static Key extractRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
  }

  private static Object mergeSum(Object left, Object right) {
    return ((Number) left).doubleValue() + ((Number) right).doubleValue();
  }

  private static Object mergeMin(Object left, Object right) {
    return Math.min(((Number) left).doubleValue(), ((Number) right).doubleValue());
  }

  private static Object mergeMax(Object left, Object right) {
    return Math.max(((Number) left).doubleValue(), ((Number) right).doubleValue());
  }

  private static Boolean mergeBoolAnd(Object left, Object right) {
    return ((Boolean) left) && ((Boolean) right);
  }

  private static Boolean mergeBoolOr(Object left, Object right) {
    return ((Boolean) left) || ((Boolean) right);
  }

  private static class MergeCounts implements AggregationUtils.Merger {

    @Override
    public Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      return other == null ? 0 : 1;
    }

    @Override
    public Object merge(Object left, Object right) {
      return ((Number) left).doubleValue() + (right == null ? 0 : 1);
    }
  }

  public interface Merger {
    /**
     * Initializes the merger based on the first input
     */
    default Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      // TODO: Initialize as a double so that if only one row is returned it matches the type when many rows are
      //       returned
      return other == null ? dataType.getNullPlaceholder() : other;
    }

    /**
     * Merges the existing aggregate (the result of {@link #initialize(Object, DataSchema.ColumnDataType)}) with
     * the new value coming in (which may be an aggregate in and of itself).
     */
    Object merge(Object agg, Object value);
  }

  /**
   * Accumulator class which accumulates the aggregated results into the group sets if any
   */
  public static class Accumulator {
    public static final Map<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> MERGERS =
        ImmutableMap.<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>>builder()
            .put("SUM", cdt -> AggregationUtils::mergeSum)
            .put("$SUM", cdt -> AggregationUtils::mergeSum)
            .put("$SUM0", cdt -> AggregationUtils::mergeSum)
            .put("MIN", cdt -> AggregationUtils::mergeMin)
            .put("$MIN", cdt -> AggregationUtils::mergeMin)
            .put("$MIN0", cdt -> AggregationUtils::mergeMin)
            .put("MAX", cdt -> AggregationUtils::mergeMax)
            .put("$MAX", cdt -> AggregationUtils::mergeMax)
            .put("$MAX0", cdt -> AggregationUtils::mergeMax)
            .put("COUNT", cdt -> new AggregationUtils.MergeCounts())
            .put("BOOL_AND", cdt -> AggregationUtils::mergeBoolAnd)
            .put("$BOOL_AND", cdt -> AggregationUtils::mergeBoolAnd)
            .put("$BOOL_AND0", cdt -> AggregationUtils::mergeBoolAnd)
            .put("BOOL_OR", cdt -> AggregationUtils::mergeBoolOr)
            .put("$BOOL_OR", cdt -> AggregationUtils::mergeBoolOr)
            .put("$BOOL_OR0", cdt -> AggregationUtils::mergeBoolOr)
            .build();

    protected final int _inputRef;
    protected final Object _literal;
    protected final Map<Key, Object> _results = new HashMap<>();
    protected final Merger _merger;
    protected final DataSchema.ColumnDataType _dataType;

    public Map<Key, Object> getResults() {
      return _results;
    }

    public Merger getMerger() {
      return _merger;
    }

    public DataSchema.ColumnDataType getDataType() {
      return _dataType;
    }

    public Accumulator(RexExpression.FunctionCall aggCall, Map<String,
        Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> merger, String functionName,
        DataSchema inputSchema) {
      // agg function operand should either be a InputRef or a Literal
      RexExpression rexExpression = toAggregationFunctionOperand(aggCall);
      if (rexExpression instanceof RexExpression.InputRef) {
        _inputRef = ((RexExpression.InputRef) rexExpression).getIndex();
        _literal = null;
        _dataType = inputSchema.getColumnDataType(_inputRef);
      } else {
        _inputRef = -1;
        _literal = ((RexExpression.Literal) rexExpression).getValue();
        _dataType = DataSchema.ColumnDataType.fromDataType(rexExpression.getDataType(), true);
      }
      _merger = merger.get(functionName).apply(_dataType);
    }

    public void accumulate(Key key, Object[] row) {
      // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
      Object currentRes = _results.get(key);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];

      if (currentRes == null) {
        _results.put(key, _merger.initialize(value, _dataType));
      } else {
        Object mergedResult = _merger.merge(currentRes, value);
        _results.put(key, mergedResult);
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall rexExpression) {
      List<RexExpression> functionOperands = rexExpression.getFunctionOperands();
      Preconditions.checkState(functionOperands.size() < 2, "aggregate functions cannot have more than one operand");
      return functionOperands.size() > 0 ? functionOperands.get(0)
          : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
    }
  }
}
