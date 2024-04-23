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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;


public abstract class ValueWindowFunction extends WindowFunction {
  public static final Map<String, Class<? extends WindowFunction>> WINDOW_FUNCTION_MAP =
      ImmutableMap.<String, Class<? extends WindowFunction>>builder()
          // Value window functions
          .put("LEAD", LeadValueWindowFunction.class)
          .put("LAG", LagValueWindowFunction.class)
          .put("FIRST_VALUE", FirstValueWindowFunction.class)
          .put("LAST_VALUE", LastValueWindowFunction.class)
          .build();

  public ValueWindowFunction(RexExpression.FunctionCall aggCall, String functionName,
      DataSchema inputSchema, WindowAggregateOperator.OrderSetInfo orderSetInfo) {
    super(aggCall, functionName, inputSchema, orderSetInfo);
  }

  protected Object extractValueFromRow(Object[] row) {
    return _inputRef == -1 ? _literal : (row == null ? null : row[_inputRef]);
  }
}
