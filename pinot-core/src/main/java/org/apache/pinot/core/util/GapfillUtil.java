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
package org.apache.pinot.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;


/**
 * Util class to encapsulate all utilites required for gapfill.
 */
public class GapfillUtil {
  private static final String AGGREGATE_GAP_FILL = "aggregategapfill";
  private static final String FILL = "fill";

  private GapfillUtil() {
  }

  public static ExpressionContext stripGapfill(ExpressionContext expression) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }
    FunctionContext function = expression.getFunction();
    String functionName = StringUtils.remove(function.getFunctionName(), '_').toLowerCase();
    if (functionName.equalsIgnoreCase(AGGREGATE_GAP_FILL) || functionName.equalsIgnoreCase(FILL)) {
      return function.getArguments().get(0);
    }
    return expression;
  }

  public static boolean isAggregateGapfill(String name) {
    return AGGREGATE_GAP_FILL.equalsIgnoreCase(name);
  }
}
