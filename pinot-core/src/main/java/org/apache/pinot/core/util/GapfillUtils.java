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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Util class to encapsulate all utilites required for gapfill.
 */
public class GapfillUtils {
  private static final String GAP_FILL = "gapfill";
  private static final String AS = "as";
  private static final String FILL = "fill";
  private static final String TIME_SERIES_ON = "timeserieson";
  private static final int STARTING_INDEX_OF_OPTIONAL_ARGS_FOR_PRE_AGGREGATE_GAP_FILL = 5;

  private GapfillUtils() {
  }

  public static ExpressionContext stripGapfill(ExpressionContext expression) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }

    FunctionContext function = expression.getFunction();
    String functionName = function.getFunctionName();
    if (functionName.equals(FILL) || functionName.equals(GAP_FILL)) {
      return function.getArguments().get(0);
    }
    return expression;
  }

  public static boolean isFill(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return FILL.equals(expressionContext.getFunction().getFunctionName());
  }

  public static boolean isTimeSeriesOn(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return TIME_SERIES_ON.equals(expressionContext.getFunction().getFunctionName());
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
      case VECTOR:
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

  public static boolean isGapfill(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      return false;
    }

    return GAP_FILL.equals(expressionContext.getFunction().getFunctionName());
  }

  private static boolean isGapfill(QueryContext queryContext) {
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      if (isGapfill(expressionContext)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the gapfill type for queryContext. Also do the validation for gapfill request.
   * @param queryContext
   */
  public static GapfillType getGapfillType(QueryContext queryContext) {
    GapfillType gapfillType = null;
    if (queryContext.getSubquery() == null) {
      if (isGapfill(queryContext)) {
        Preconditions.checkArgument(queryContext.getAggregationFunctions() == null,
            "Aggregation and Gapfill can not be in the same sql statement.");
        gapfillType = GapfillType.GAP_FILL;
      }
    } else if (isGapfill(queryContext)) {
      Preconditions.checkArgument(queryContext.getSubquery().getAggregationFunctions() != null,
          "Select and Gapfill should be in the same sql statement.");
      Preconditions.checkArgument(queryContext.getSubquery().getSubquery() == null,
          "There is no three levels nesting sql when the outer query is gapfill.");
      gapfillType = GapfillType.AGGREGATE_GAP_FILL;
    } else if (isGapfill(queryContext.getSubquery())) {
      if (queryContext.getAggregationFunctions() == null) {
        gapfillType = GapfillType.GAP_FILL_SELECT;
      } else if (queryContext.getSubquery().getSubquery() == null) {
        gapfillType = GapfillType.GAP_FILL_AGGREGATE;
      } else {
        Preconditions.checkArgument(queryContext.getSubquery().getSubquery().getAggregationFunctions() != null,
            "Select cannot happen before gapfill.");
        gapfillType = GapfillType.AGGREGATE_GAP_FILL_AGGREGATE;
      }
    }

    if (gapfillType == null) {
      return gapfillType;
    }

    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext, gapfillType);

    Preconditions.checkArgument(gapFillSelection != null && gapFillSelection.getFunction() != null,
        "Gapfill Expression should be function.");
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    Preconditions.checkArgument(args.size() > 5, "Gapfill does not have correct number of arguments.");
    Preconditions.checkArgument(args.get(1).getLiteral() != null,
        "The second argument of Gapfill should be TimeFormatter.");
    Preconditions.checkArgument(args.get(2).getLiteral() != null,
        "The third argument of Gapfill should be start time.");
    Preconditions.checkArgument(args.get(3).getLiteral() != null,
        "The fourth argument of Gapfill should be end time.");
    Preconditions.checkArgument(args.get(4).getLiteral() != null,
        "The fifth argument of Gapfill should be time bucket size.");

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    Preconditions.checkArgument(timeseriesOn != null, "The TimeSeriesOn expressions should be specified.");

    if (queryContext.getAggregationFunctions() == null) {
      return gapfillType;
    }

    List<ExpressionContext> groupbyExpressions = queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    List<ExpressionContext> innerSelections = queryContext.getSubquery().getSelectExpressions();
    String timeBucketCol = null;
    List<String> strAlias = queryContext.getSubquery().getAliasList();
    for (int i = 0; i < innerSelections.size(); i++) {
      ExpressionContext innerSelection = innerSelections.get(i);
      if (GapfillUtils.isGapfill(innerSelection)) {
        if (strAlias.get(i) != null) {
          timeBucketCol = strAlias.get(i);
        } else {
          timeBucketCol = innerSelection.getFunction().getArguments().get(0).toString();
        }
        break;
      }
    }

    Preconditions.checkArgument(timeBucketCol != null, "No Group By timebucket.");

    boolean findTimeBucket = false;
    for (ExpressionContext groupbyExp : groupbyExpressions) {
      if (timeBucketCol.equals(groupbyExp.toString())) {
        findTimeBucket = true;
        break;
      }
    }

    Preconditions.checkArgument(findTimeBucket, "No Group By timebucket.");
    return gapfillType;
  }

  private static ExpressionContext findGapfillExpressionContext(QueryContext queryContext) {
    for (ExpressionContext expressionContext : queryContext.getSelectExpressions()) {
      if (isGapfill(expressionContext)) {
        return expressionContext;
      }
    }
    return null;
  }

  public static ExpressionContext getGapfillExpressionContext(QueryContext queryContext, GapfillType gapfillType) {
    if (gapfillType == GapfillType.AGGREGATE_GAP_FILL || gapfillType == GapfillType.GAP_FILL) {
      return findGapfillExpressionContext(queryContext);
    } else if (gapfillType == GapfillType.GAP_FILL_AGGREGATE || gapfillType == GapfillType.AGGREGATE_GAP_FILL_AGGREGATE
        || gapfillType == GapfillType.GAP_FILL_SELECT) {
      return findGapfillExpressionContext(queryContext.getSubquery());
    } else {
      return null;
    }
  }

  public static int findTimeBucketColumnIndex(QueryContext queryContext, GapfillType gapfillType) {
    if (gapfillType == GapfillType.GAP_FILL_AGGREGATE || gapfillType == GapfillType.GAP_FILL_SELECT
        || gapfillType == GapfillType.AGGREGATE_GAP_FILL_AGGREGATE) {
      queryContext = queryContext.getSubquery();
    }
    List<ExpressionContext> expressionContexts = queryContext.getSelectExpressions();
    for (int i = 0; i < expressionContexts.size(); i++) {
      if (isGapfill(expressionContexts.get(i))) {
        return i;
      }
    }
    return -1;
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

  public static PinotQuery stripGapfill(PinotQuery pinotQuery) {
    if (pinotQuery.getDataSource() == null || (pinotQuery.getDataSource().getSubquery() == null && !hasGapfill(
        pinotQuery))) {
      return pinotQuery;
    }

    // Carry over the query and debug options from the original query
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    Map<String, String> debugOptions = pinotQuery.getDebugOptions();

    while (pinotQuery.getDataSource().getSubquery() != null) {
      pinotQuery = pinotQuery.getDataSource().getSubquery();
    }

    PinotQuery strippedPinotQuery = new PinotQuery(pinotQuery);
    List<Expression> selectList = strippedPinotQuery.getSelectList();
    for (int i = 0; i < selectList.size(); i++) {
      Expression select = selectList.get(i);
      if (select.getType() != ExpressionType.FUNCTION) {
        continue;
      }
      if (GAP_FILL.equals(select.getFunctionCall().getOperator())) {
        selectList.set(i, select.getFunctionCall().getOperands().get(0));
        break;
      }
      if (AS.equals(select.getFunctionCall().getOperator())
          && select.getFunctionCall().getOperands().get(0).getType() == ExpressionType.FUNCTION && GAP_FILL.equals(
          select.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator())) {
        select.getFunctionCall().getOperands()
            .set(0, select.getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0));
        break;
      }
    }

    List<Expression> orderByList = strippedPinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression orderBy : orderByList) {
        if (orderBy.getType() != ExpressionType.FUNCTION) {
          continue;
        }
        if (orderBy.getFunctionCall().getOperands().get(0).getType() == ExpressionType.FUNCTION && GAP_FILL.equals(
            orderBy.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator())) {
          orderBy.getFunctionCall().getOperands()
              .set(0, orderBy.getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0));
          break;
        }
      }
    }

    strippedPinotQuery.setQueryOptions(queryOptions);
    strippedPinotQuery.setDebugOptions(debugOptions);
    return strippedPinotQuery;
  }

  private static boolean hasGapfill(PinotQuery pinotQuery) {
    for (Expression select : pinotQuery.getSelectList()) {
      if (select.getType() != ExpressionType.FUNCTION) {
        continue;
      }
      if (GAP_FILL.equals(select.getFunctionCall().getOperator())) {
        return true;
      }
      if (AS.equals(select.getFunctionCall().getOperator())
          && select.getFunctionCall().getOperands().get(0).getType() == ExpressionType.FUNCTION && GAP_FILL.equals(
          select.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator())) {
        return true;
      }
    }
    return false;
  }

  public enum GapfillType {
    // one sql query with gapfill only
    GAP_FILL,
    // gapfill as subquery, the outer query may have the filter
    GAP_FILL_SELECT,
    // gapfill as subquery, the outer query has the aggregation
    GAP_FILL_AGGREGATE,
    // aggregation as subqery, the outer query is gapfill
    AGGREGATE_GAP_FILL,
    // aggegration as second nesting subquery, gapfill as first nesting subquery, different aggregation as outer query
    AGGREGATE_GAP_FILL_AGGREGATE
  }

  public enum FillType {
    FILL_DEFAULT_VALUE, FILL_PREVIOUS_VALUE,
  }
}
