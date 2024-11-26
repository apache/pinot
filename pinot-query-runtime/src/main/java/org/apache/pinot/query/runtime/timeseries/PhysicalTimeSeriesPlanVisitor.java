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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class PhysicalTimeSeriesPlanVisitor {
  public static final PhysicalTimeSeriesPlanVisitor INSTANCE = new PhysicalTimeSeriesPlanVisitor();

  private QueryExecutor _queryExecutor;
  private ExecutorService _executorService;
  private ServerMetrics _serverMetrics;

  private PhysicalTimeSeriesPlanVisitor() {
  }

  public void init(QueryExecutor queryExecutor, ExecutorService executorService, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _serverMetrics = serverMetrics;
  }

  public BaseTimeSeriesOperator compile(BaseTimeSeriesPlanNode rootNode, TimeSeriesExecutionContext context) {
    // Step-1: Replace scan filter project with our physical plan node with Pinot Core and Runtime context
    initLeafPlanNode(rootNode, context);
    // Step-2: Trigger recursive operator generation
    return rootNode.run();
  }

  public void initLeafPlanNode(BaseTimeSeriesPlanNode planNode, TimeSeriesExecutionContext context) {
    for (int index = 0; index < planNode.getChildren().size(); index++) {
      BaseTimeSeriesPlanNode childNode = planNode.getChildren().get(index);
      if (childNode instanceof LeafTimeSeriesPlanNode) {
        LeafTimeSeriesPlanNode leafNode = (LeafTimeSeriesPlanNode) childNode;
        List<String> segments = context.getPlanIdToSegmentsMap().get(leafNode.getId());
        ServerQueryRequest serverQueryRequest = compileLeafServerQueryRequest(leafNode, segments, context);
        TimeSeriesPhysicalTableScan physicalTableScan = new TimeSeriesPhysicalTableScan(context, childNode.getId(),
            serverQueryRequest, _queryExecutor, _executorService);
        planNode.getChildren().set(index, physicalTableScan);
      } else {
        initLeafPlanNode(childNode, context);
      }
    }
  }

  public ServerQueryRequest compileLeafServerQueryRequest(LeafTimeSeriesPlanNode leafNode, List<String> segments,
      TimeSeriesExecutionContext context) {
    return new ServerQueryRequest(compileQueryContext(leafNode, context),
        segments, getServerQueryRequestMetadataMap(context), _serverMetrics);
  }

  @VisibleForTesting
  QueryContext compileQueryContext(LeafTimeSeriesPlanNode leafNode, TimeSeriesExecutionContext context) {
    FilterContext filterContext =
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(
            leafNode.getEffectiveFilter(context.getInitialTimeBuckets())));
    List<ExpressionContext> groupByExpressions = leafNode.getGroupByExpressions().stream()
        .map(RequestContextUtils::getExpression).collect(Collectors.toList());
    ExpressionContext valueExpression = RequestContextUtils.getExpression(leafNode.getValueExpression());
    ExpressionContext timeExpression = buildTimeTransform(leafNode.getTimeColumn(), leafNode.getTimeUnit(),
        context.getInitialTimeBuckets(), leafNode.getOffsetSeconds() == null ? 0L : leafNode.getOffsetSeconds());
    ExpressionContext aggFunctionExpr = buildAggregationExpr(toPinotCoreAggregation(leafNode.getAggInfo()),
        valueExpression, timeExpression, context.getInitialTimeBuckets().getNumBuckets());
    return new QueryContext.Builder()
        .setTableName(leafNode.getTableName())
        .setFilter(filterContext)
        .setGroupByExpressions(groupByExpressions)
        .setSelectExpressions(List.of(aggFunctionExpr))
        .setQueryOptions(ImmutableMap.of(QueryOptionKey.TIMEOUT_MS, Long.toString(context.getTimeoutMs())))
        .setAliasList(Collections.emptyList())
        .setLimit(Integer.MAX_VALUE)
        .build();
  }

  Map<String, String> getServerQueryRequestMetadataMap(TimeSeriesExecutionContext context) {
    Map<String, String> result = new HashMap<>();
    result.put(MetadataKeys.BROKER_ID, context.getMetadataMap().get(MetadataKeys.BROKER_ID));
    result.put(MetadataKeys.REQUEST_ID, context.getMetadataMap().get(MetadataKeys.REQUEST_ID));
    return result;
  }

  private ExpressionContext buildAggregationExpr(AggregationFunctionType functionType,
      ExpressionContext valueExpression, ExpressionContext timeExpression, int numBuckets) {
    ExpressionContext literalExpr = ExpressionContext.forLiteral(Literal.intValue(numBuckets));
    FunctionContext aggFunction = new FunctionContext(FunctionContext.Type.AGGREGATION,
        functionType.getName(), List.of(valueExpression, timeExpression, literalExpr));
    return ExpressionContext.forFunction(aggFunction);
  }

  private ExpressionContext buildTimeTransform(String timeColumn, TimeUnit timeUnit, TimeBuckets timeBuckets,
      long offsetSeconds) {
    final String functionName = TransformFunctionType.TIMESERIES_BUCKET_INDEX.name();
    final List<ExpressionContext> arguments = new ArrayList<>(4);
    arguments.add(RequestContextUtils.getExpression(timeColumn));
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(timeUnit.toString())));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getTimeBuckets()[0])));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getBucketSize().getSeconds())));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(offsetSeconds)));
    return ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, functionName, arguments));
  }

  private AggregationFunctionType toPinotCoreAggregation(AggInfo aggInfo) {
    // TODO(timeseries): This is hacky.
    switch (aggInfo.getAggFunction().toUpperCase()) {
      case "SUM":
        return AggregationFunctionType.TIMESERIESSUM;
      case "MIN":
        return AggregationFunctionType.TIMESERIESMIN;
      case "MAX":
        return AggregationFunctionType.TIMESERIESMAX;
      default:
        throw new UnsupportedOperationException("Unsupported agg function type: " + aggInfo.getAggFunction());
    }
  }
}
