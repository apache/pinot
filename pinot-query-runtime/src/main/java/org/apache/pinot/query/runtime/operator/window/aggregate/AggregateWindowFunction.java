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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils.Merger;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;


public class AggregateWindowFunction extends WindowFunction {
  private final Merger _merger;

  public AggregateWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, boolean partitionByOnly) {
    super(aggCall, inputSchema, collations, partitionByOnly);
    String functionName = aggCall.getFunctionName();
    Function<ColumnDataType, Merger> mergerCreator = AggregationUtils.Accumulator.MERGERS.get(functionName);
    Preconditions.checkArgument(mergerCreator != null, "Unsupported aggregate function: %s", functionName);
    _merger = mergerCreator.apply(_dataType);
  }

  @Override
  public final List<Object> processRows(List<Object[]> rows) {
    if (_partitionByOnly) {
      return processPartitionOnlyRows(rows);
    } else {
      return processRowsInternal(rows);
    }
  }

  protected List<Object> processPartitionOnlyRows(List<Object[]> rows) {
    Object mergedResult = null;
    for (Object[] row : rows) {
      Object value = _inputRef == -1 ? _literal : row[_inputRef];
      if (value == null) {
        continue;
      }
      if (mergedResult == null) {
        mergedResult = _merger.init(value, _dataType);
      } else {
        mergedResult = _merger.merge(mergedResult, value);
      }
    }
    return Collections.nCopies(rows.size(), mergedResult);
  }

  protected List<Object> processRowsInternal(List<Object[]> rows) {
    Key emptyOrderKey = AggregationUtils.extractEmptyKey();
    OrderKeyResult orderByResult = new OrderKeyResult();
    for (Object[] row : rows) {
      // Only need to accumulate the aggregate function values for RANGE type. ROW type can be calculated as
      // we output the rows since the aggregation value depends on the neighboring rows.
      Key orderKey = (_partitionByOnly && _orderKeys.length == 0) ? emptyOrderKey
          : AggregationUtils.extractRowKey(row, _orderKeys);

      Key previousOrderKeyIfPresent = orderByResult.getPreviousOrderByKey();
      Object currentRes =
          previousOrderKeyIfPresent == null ? null : orderByResult.getOrderByResults().get(previousOrderKeyIfPresent);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];
      if (currentRes == null) {
        orderByResult.addOrderByResult(orderKey, _merger.init(value, _dataType));
      } else {
        orderByResult.addOrderByResult(orderKey, _merger.merge(currentRes, value));
      }
    }
    List<Object> results = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      // Only need to accumulate the aggregate function values for RANGE type. ROW type can be calculated as
      // we output the rows since the aggregation value depends on the neighboring rows.
      Key orderKey = (_partitionByOnly && _orderKeys.length == 0) ? emptyOrderKey
          : AggregationUtils.extractRowKey(row, _orderKeys);
      Object value = orderByResult.getOrderByResults().get(orderKey);
      results.add(value);
    }
    return results;
  }

  static class OrderKeyResult {
    final Map<Key, Object> _orderByResults;
    Key _previousOrderByKey;

    OrderKeyResult() {
      _orderByResults = new HashMap<>();
      _previousOrderByKey = null;
    }

    public void addOrderByResult(Key orderByKey, Object value) {
      // We expect to get the rows in order based on the ORDER BY key so it is safe to blindly assign the
      // current key as the previous key
      _orderByResults.put(orderByKey, value);
      _previousOrderByKey = orderByKey;
    }

    public Map<Key, Object> getOrderByResults() {
      return _orderByResults;
    }

    public Key getPreviousOrderByKey() {
      return _previousOrderByKey;
    }
  }
}
