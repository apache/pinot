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

import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Util class to encapsulate all utilites required for gapfill.
 */
public class GapfillUtils {
  private static final String POST_AGGREGATE_GAP_FILL = "postaggregategapfill";
  private static final String FILL = "fill";

  private GapfillUtils() {
  }

  public static ExpressionContext stripGapfill(ExpressionContext expression) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }

    FunctionContext function = expression.getFunction();
    String functionName = canonicalizeFunctionName(function.getFunctionName());
    if (functionName.equals(POST_AGGREGATE_GAP_FILL) || functionName.equals(FILL)) {
      return function.getArguments().get(0);
    }
    return expression;
  }

  public static boolean isPostAggregateGapfill(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return POST_AGGREGATE_GAP_FILL.equals(canonicalizeFunctionName(expressionContext.getFunction().getFunctionName()));
  }

  public static boolean isPostAggregateGapfill(QueryContext queryContext) {
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      if (isPostAggregateGapfill(expressionContext)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isFill(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return FILL.equals(canonicalizeFunctionName(expressionContext.getFunction().getFunctionName()));
  }

  static public enum FillType {
    FILL_DEFAULT_VALUE,
    FILL_PREVIOUS_VALUE,
  }

  /**
   * The default gapfill value for each column type.
   */
  static public Serializable getDefaultValue(DataSchema.ColumnDataType dataType) {
    switch (dataType) {
      // Single-value column
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case TIMESTAMP:
        return dataType.convertAndFormat(0);
      case STRING:
      case JSON:
      case BYTES:
        return "";
      case INT_ARRAY:
        return new int[0];
      case LONG_ARRAY:
        return new long[0];
      case FLOAT_ARRAY:
        return new float[0];
      case DOUBLE_ARRAY:
        return new double[0];
      case STRING_ARRAY:
      case TIMESTAMP_ARRAY:
        return new String[0];
      case BOOLEAN_ARRAY:
        return new boolean[0];
      case BYTES_ARRAY:
        return new byte[0][0];
      default:
        throw new IllegalStateException(String.format("Cannot provide the default value for the type: %s", dataType));
    }
  }

  private static String canonicalizeFunctionName(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }
}
