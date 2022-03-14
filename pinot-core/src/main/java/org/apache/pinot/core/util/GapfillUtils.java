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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;


/**
 * Util class to encapsulate all utilites required for gapfill.
 */
public class GapfillUtils {
  private static final String POST_AGGREGATE_GAP_FILL = "postaggregategapfill";
  private static final String GAP_FILL = "gapfill";
  private static final String AS = "as";
  private static final String FILL = "fill";
  private static final String TIME_SERIES_ON = "timeSeriesOn";
  private static final int STARTING_INDEX_OF_OPTIONAL_ARGS_FOR_PRE_AGGREGATE_GAP_FILL = 5;

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

  /**
   * Get the gapfill type for queryContext. Also do the validation for gapfill request.
   * @param queryContext
   */
  public static void setGapfillType(QueryContext queryContext) {
    GapfillType gapfillType = null;
    if (queryContext.getSubQueryContext() == null) {
      if (isGapfill(queryContext)) {
        Preconditions.checkArgument(queryContext.getAggregationFunctions() == null,
            "Aggregation and Gapfill can not be in the same sql statement.");
        gapfillType = GapfillType.GAP_FILL;
      }
    } else if (isGapfill(queryContext)) {
      Preconditions.checkArgument(queryContext.getSubQueryContext().getAggregationFunctions() != null,
          "Select and Gapfill should be in the same sql statement.");
      Preconditions.checkArgument(queryContext.getSubQueryContext().getSubQueryContext() == null,
          "There is no three levels nesting sql when the outer query is gapfill.");
      gapfillType = GapfillType.AGGREGATE_GAP_FILL;
    } else if (isGapfill(queryContext.getSubQueryContext())) {
      if (queryContext.getAggregationFunctions() == null) {
        gapfillType = GapfillType.GAP_FILL_SELECT;
      } else if (queryContext.getSubQueryContext().getSubQueryContext() == null) {
        gapfillType = GapfillType.GAP_FILL_AGGREGATE;
      } else {
        Preconditions
            .checkArgument(queryContext.getSubQueryContext().getSubQueryContext().getAggregationFunctions() != null,
                "Select cannot happen before gapfill.");
        gapfillType = GapfillType.AGGREGATE_GAP_FILL_AGGREGATE;
      }
    }

    queryContext.setGapfillType(gapfillType);
    if (gapfillType == null) {
      return;
    }

    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext);

    Preconditions.checkArgument(gapFillSelection != null && gapFillSelection.getFunction() != null,
        "Gapfill Expression should be function.");
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    Preconditions.checkArgument(args.size() > 5, "PreAggregateGapFill does not have correct number of arguments.");
    Preconditions.checkArgument(args.get(1).getLiteral() != null,
        "The second argument of PostAggregateGapFill should be TimeFormatter.");
    Preconditions.checkArgument(args.get(2).getLiteral() != null,
        "The third argument of PostAggregateGapFill should be start time.");
    Preconditions.checkArgument(args.get(3).getLiteral() != null,
        "The fourth argument of PostAggregateGapFill should be end time.");
    Preconditions.checkArgument(args.get(4).getLiteral() != null,
        "The fifth argument of PostAggregateGapFill should be time bucket size.");

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    Preconditions.checkArgument(timeseriesOn != null, "The TimeSeriesOn expressions should be specified.");

    if (queryContext.getAggregationFunctions() == null) {
      return;
    }

    List<ExpressionContext> groupbyExpressions = queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    List<ExpressionContext> innerSelections = queryContext.getSubQueryContext().getSelectExpressions();
    String timeBucketCol = null;
    List<String> strAlias = queryContext.getSubQueryContext().getAliasList();
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
  }

  public static ExpressionContext findGapfillExpressionContext(QueryContext queryContext) {
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
    } else if (gapfillType == GapfillType.GAP_FILL_AGGREGATE || gapfillType == GapfillType.AGGREGATE_GAP_FILL_AGGREGATE
        || gapfillType == GapfillType.GAP_FILL_SELECT) {
      return findGapfillExpressionContext(queryContext.getSubQueryContext());
    } else {
      return null;
    }
  }

  public static int findGapfillExpressionContextIndex(QueryContext queryContext) {
    List<ExpressionContext> expressionContexts = queryContext.getSelectExpressions();
    for (int i = 0; i < expressionContexts.size(); i++) {
      if (isGapfill(expressionContexts.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public static int findGapfillExpressionContextIndexWithSubquery(QueryContext queryContext) {
    List<ExpressionContext> expressionContexts = queryContext.getSelectExpressions();
    for (int i = 0; i < expressionContexts.size(); i++) {
      if (isGapfill(expressionContexts.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public static int findTimeBucketColumnIndex(QueryContext queryContext) {
    GapfillType gapfillType = queryContext.getGapfillType();
    if (gapfillType == GapfillType.GAP_FILL) {
      return findGapfillExpressionContextIndex(queryContext);
    } else if (gapfillType == GapfillType.GAP_FILL_AGGREGATE || gapfillType == GapfillType.GAP_FILL_SELECT) {
      return findGapfillExpressionContextIndex(queryContext.getSubQueryContext());
    } else if (gapfillType == GapfillType.AGGREGATE_GAP_FILL) {
      return findGapfillExpressionContextIndexWithSubquery(queryContext);
    } else if (gapfillType == GapfillType.AGGREGATE_GAP_FILL_AGGREGATE) {
      return findGapfillExpressionContextIndexWithSubquery(queryContext.getSubQueryContext());
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

  public static List<ExpressionContext> findGroupByExpressions(QueryContext queryContext) {
    ExpressionContext gapFillSelection = GapfillUtils.findGapfillExpressionContext(queryContext);
    if (gapFillSelection == null) {
      return null;
    }
    List<ExpressionContext> groupByExpressions = new ArrayList<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    groupByExpressions.add(gapFillSelection.getFunction().getArguments().get(0));
    groupByExpressions.addAll(timeseriesOn.getFunction().getArguments());
    return groupByExpressions;
  }

  public static List<ExpressionContext> getGroupByExpressions(QueryContext queryContext) {
    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext);
    if (gapFillSelection == null) {
      return null;
    }
    List<ExpressionContext> groupByExpressions = new ArrayList<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    groupByExpressions.add(gapFillSelection.getFunction().getArguments().get(0));
    groupByExpressions.addAll(timeseriesOn.getFunction().getArguments());
    return groupByExpressions;
  }

  public static String getTableName(PinotQuery pinotQuery) {
    while (pinotQuery.getDataSource().getSubquery() != null) {
      pinotQuery = pinotQuery.getDataSource().getSubquery();
    }
    return pinotQuery.getDataSource().getTableName();
  }

  public static BrokerRequest stripGapfill(BrokerRequest brokerRequest) {
    if (brokerRequest.getPinotQuery().getDataSource() == null) {
      return brokerRequest;
    }
    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    GapfillUtils.GapfillType gapfillType = queryContext.getGapfillType();
    if (gapfillType == null) {
      return brokerRequest;
    }
    switch (gapfillType) {
      // one sql query with gapfill only
      case GAP_FILL:
        return stripGapfill(brokerRequest.getPinotQuery());
      // gapfill as subquery, the outer query may have the filter
      case GAP_FILL_SELECT:
        // gapfill as subquery, the outer query has the aggregation
      case GAP_FILL_AGGREGATE:
        // aggregation as subqery, the outer query is gapfill
      case AGGREGATE_GAP_FILL:
        return stripGapfill(brokerRequest.getPinotQuery().getDataSource().getSubquery());
      // aggegration as second nesting subquery, gapfill as first nesting subquery, different aggregation as outer query
      case AGGREGATE_GAP_FILL_AGGREGATE:
        return stripGapfill(brokerRequest.getPinotQuery().getDataSource().getSubquery().getDataSource().getSubquery());
      default:
        return brokerRequest;
    }
  }

  private static BrokerRequest stripGapfill(PinotQuery pinotQuery) {
    PinotQuery copy = new PinotQuery(pinotQuery);
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(copy);
    // Set table name in broker request because it is used for access control, query routing etc.
    DataSource dataSource = copy.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }
    List<Expression> selectList = copy.getSelectList();
    for (int i = 0; i < selectList.size(); i++) {
      Expression select = selectList.get(i);
      if (select.getType() != ExpressionType.FUNCTION) {
        continue;
      }
      if (GAP_FILL.equalsIgnoreCase(select.getFunctionCall().getOperator())) {
        selectList.set(i, select.getFunctionCall().getOperands().get(0));
        break;
      }
      if (AS.equalsIgnoreCase(select.getFunctionCall().getOperator())
          && select.getFunctionCall().getOperands().get(0).getType() == ExpressionType.FUNCTION
          && GAP_FILL.equalsIgnoreCase(select.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator())) {
        select.getFunctionCall().getOperands().set(0,
            select.getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0));
        break;
      }
    }
    List<Expression> orderByList = copy.getOrderByList();
    for (int i = 0; i < orderByList.size(); i++) {
      Expression orderBy = orderByList.get(i);
      if (orderBy.getType() != ExpressionType.FUNCTION) {
        continue;
      }
      if (orderBy.getFunctionCall().getOperands().get(0).getType() == ExpressionType.FUNCTION
          && GAP_FILL.equalsIgnoreCase(
              orderBy.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator())) {
        orderBy.getFunctionCall().getOperands().set(0,
            orderBy.getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0));
        break;
      }
    }
    return brokerRequest;
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
