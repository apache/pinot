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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final String GAP_FILL = "gapfill";
  private static final String FILL = "fill";
  private static final String TIME_SERIES_ON = "timeSeriesOn";
  private static final int STARTING_INDEX_OF_OPTIONAL_ARGS_FOR_PRE_AGGREGATE_GAP_FILL = 5;

  public enum GapfillType {
    // one sql query with gapfill only
    GAP_FILL,
    // gapfill as subquery, the outer query may have the filter
    GAP_FILL_SELECT,
    // gapfill as subquery, the outer query has the aggregation
    GAP_FILL_AGGREGATE,
    // aggregation as subqery, the outer query is gapfill
    AGGREGATE_GAP_FILL,
    // aggegration as second nesting subquery, gapfill as fist nesting subquery, different aggregation as outer query
    AGGREGATE_GAP_FILL_AGGREGATE
  }

  private GapfillUtils() {
  }

  public static ExpressionContext stripGapfill(ExpressionContext expression) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }

    FunctionContext function = expression.getFunction();
    String functionName = canonicalizeFunctionName(function.getFunctionName());
    if (functionName.equals(POST_AGGREGATE_GAP_FILL) || functionName.equals(FILL) || functionName.equals(GAP_FILL)) {
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

    return FILL.equalsIgnoreCase(canonicalizeFunctionName(expressionContext.getFunction().getFunctionName()));
  }

  public static boolean isTimeSeriesOn(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return TIME_SERIES_ON.equalsIgnoreCase(canonicalizeFunctionName(expressionContext.getFunction().getFunctionName()));
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

  public static boolean isGapfill(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return GAP_FILL.equals(canonicalizeFunctionName(expressionContext.getFunction().getFunctionName()));
  }

  private static boolean isGapfill(QueryContext queryContext) {
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      if (isGapfill(expressionContext)) {
        return true;
      }
    }
    return false;
  }

  public static GapfillType getGapfillType(QueryContext queryContext) {
    if (queryContext.getSubQueryContext() == null) {
      if (isGapfill(queryContext)) {
        Preconditions.checkArgument(queryContext.getAggregationFunctions() == null,
            "Aggregation and Gapfill can not be in the same sql statement.");
        return GapfillType.GAP_FILL;
      } else {
        return null;
      }
    } else if (isGapfill(queryContext)) {
      Preconditions.checkArgument(queryContext.getSubQueryContext().getAggregationFunctions() != null,
          "Select and Gapfill should be in the same sql statement.");
      Preconditions.checkArgument(queryContext.getSubQueryContext().getSubQueryContext() == null,
          "There is no three levels nesting sql when the outer query is gapfill.");
      return GapfillType.AGGREGATE_GAP_FILL;
    } else if (isGapfill(queryContext.getSubQueryContext())) {
      if (queryContext.getAggregationFunctions() == null) {
        return GapfillType.GAP_FILL_SELECT;
      } else if (queryContext.getSubQueryContext().getSubQueryContext() == null) {
        return GapfillType.GAP_FILL_AGGREGATE;
      } else {
        Preconditions
            .checkArgument(queryContext.getSubQueryContext().getSubQueryContext().getAggregationFunctions() != null,
                "Select cannot happen before gapfill.");
        return GapfillType.AGGREGATE_GAP_FILL_AGGREGATE;
      }
    } else {
      return null;
    }
  }

  private static ExpressionContext findGapfillExpressionContext(QueryContext queryContext) {
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      if (isGapfill(expressionContext)) {
        return expressionContext;
      }
    }
    return null;
  }

  public static ExpressionContext getGapfillExpressionContext(QueryContext queryContext) {
    GapfillType gapfillType = queryContext.getGapfillType();
    if (gapfillType == GapfillType.AGGREGATE_GAP_FILL || gapfillType == GapfillType.GAP_FILL) {
      return findGapfillExpressionContext(queryContext);
    } else if (gapfillType == GapfillType.GAP_FILL_AGGREGATE
        || gapfillType == GapfillType.AGGREGATE_GAP_FILL_AGGREGATE
        || gapfillType == GapfillType.GAP_FILL_SELECT) {
      return findGapfillExpressionContext(queryContext.getSubQueryContext());
    } else {
      return null;
    }
  }

  public static ExpressionContext getTimeSeriesOnExpressionContext(ExpressionContext gapFillSelection) {
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    for (int i = STARTING_INDEX_OF_OPTIONAL_ARGS_FOR_PRE_AGGREGATE_GAP_FILL; i < args.size(); i++) {
      if (GapfillUtils.isTimeSeriesOn(args.get(i))) {
        return args.get(i);
      }
    }
    return null;
  }

  public static Map<String, ExpressionContext> getFillExpressions(ExpressionContext gapFillSelection) {
    Map<String, ExpressionContext> fillExpressions = new HashMap<>();
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    for (int i = STARTING_INDEX_OF_OPTIONAL_ARGS_FOR_PRE_AGGREGATE_GAP_FILL; i < args.size(); i++) {
      if (GapfillUtils.isFill(args.get(i))) {
        ExpressionContext fillExpression = args.get(i);
        fillExpressions.put(fillExpression.getFunction().getArguments().get(0).getIdentifier(), fillExpression);
      }
    }
    return fillExpressions;
  }

  public static List<ExpressionContext> getGroupByExpressions(QueryContext queryContext) {
    ExpressionContext gapFillSelection =
        GapfillUtils.getGapfillExpressionContext(queryContext);
    if (gapFillSelection == null) {
      return null;
    }
    List<ExpressionContext> groupByExpressions = new ArrayList<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    groupByExpressions.add(gapFillSelection.getFunction().getArguments().get(0));
    groupByExpressions.addAll(timeseriesOn.getFunction().getArguments());
    return groupByExpressions;
  }
}
