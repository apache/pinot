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
package org.apache.pinot.query.runtime.operator.window;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;


public abstract class ValueWindowFunction implements WindowFunction {
  public static final Map<String, Class<? extends ValueWindowFunction>> VALUE_WINDOW_FUNCTION_MAP =
      ImmutableMap.<String, Class<? extends ValueWindowFunction>>builder()
          .put("LEAD", LeadValueWindowFunction.class)
          .put("LAG", LagValueWindowFunction.class)
          .put("FIRST_VALUE", FirstValueWindowFunction.class)
          .put("LAST_VALUE", LastValueWindowFunction.class)
          .build();

  /**
   * @param rowId           Row id to process
   * @param partitionedRows List of rows for reference
   * @return Row with the window function applied
   */
  public abstract Object[] processRow(int rowId, List<Object[]> partitionedRows);

  public static ValueWindowFunction construnctValueWindowFunction(String functionName) {
    Class<? extends ValueWindowFunction> valueWindowFunctionClass = VALUE_WINDOW_FUNCTION_MAP.get(functionName);
    if (valueWindowFunctionClass == null) {
      return null;
    }
    try {
      return valueWindowFunctionClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException("Failed to instantiate ValueWindowFunction for function: " + functionName, e);
    }
  }
}
