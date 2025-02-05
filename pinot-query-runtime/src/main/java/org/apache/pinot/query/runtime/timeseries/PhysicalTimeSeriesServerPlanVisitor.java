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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.transform.function.TimeSeriesBucketTransformFunction;
import org.apache.pinot.core.query.aggregation.function.TimeSeriesAggregationFunction;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class PhysicalTimeSeriesServerPlanVisitor {
  private QueryExecutor _queryExecutor;
  private ExecutorService _executorService;
  private ServerMetrics _serverMetrics;

  // Warning: Don't use singleton access pattern, since Quickstarts run in a single JVM and spawn multiple broker/server
  public PhysicalTimeSeriesServerPlanVisitor(QueryExecutor queryExecutor, ExecutorService executorService,
      ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _serverMetrics = serverMetrics;
  }

  public BaseTimeSeriesOperator compile(BaseTimeSeriesPlanNode rootNode, TimeSeriesExecutionContext context) {
    // Step-1: Replace leaf node with our physical plan node with Pinot Core and Runtime context
    rootNode = initLeafPlanNode(rootNode, context);
    // Step-2: Trigger recursive operator generation
    return rootNode.run();
  }

  public BaseTimeSeriesPlanNode initLeafPlanNode(BaseTimeSeriesPlanNode planNode, TimeSeriesExecutionContext context) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      return convertLeafToPhysicalTableScan((LeafTimeSeriesPlanNode) planNode, context);
    }
    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (int index = 0; index < planNode.getInputs().size(); index++) {
      BaseTimeSeriesPlanNode childNode = planNode.getInputs().get(index);
      if (childNode instanceof LeafTimeSeriesPlanNode) {
        LeafTimeSeriesPlanNode leafNode = (LeafTimeSeriesPlanNode) childNode;
        newInputs.add(convertLeafToPhysicalTableScan(leafNode, context));
      } else {
        newInputs.add(initLeafPlanNode(childNode, context));
      }
    }
    return planNode.withInputs(newInputs);
  }

  private TimeSeriesPhysicalTableScan convertLeafToPhysicalTableScan(LeafTimeSeriesPlanNode leafNode,
      TimeSeriesExecutionContext context) {
    List<String> segments = context.getPlanIdToSegmentsMap().getOrDefault(leafNode.getId(), Collections.emptyList());
    ServerQueryRequest serverQueryRequest = compileLeafServerQueryRequest(leafNode, segments, context);
    return new TimeSeriesPhysicalTableScan(context, leafNode.getId(), serverQueryRequest, _queryExecutor,
        _executorService);
  }

  public ServerQueryRequest compileLeafServerQueryRequest(LeafTimeSeriesPlanNode leafNode, List<String> segments,
      TimeSeriesExecutionContext context) {
    return new ServerQueryRequest(compileQueryContext(leafNode, context),
        segments, getServerQueryRequestMetadataMap(context), _serverMetrics);
  }

  /**
   * Create a transform expression, and link it with an aggregation function.
   */
  @VisibleForTesting
  QueryContext compileQueryContext(LeafTimeSeriesPlanNode leafNode, TimeSeriesExecutionContext context) {
    FilterContext filterContext =
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(
            leafNode.getEffectiveFilter(context.getInitialTimeBuckets())));
    List<ExpressionContext> groupByExpressions = leafNode.getGroupByExpressions().stream()
        .map(RequestContextUtils::getExpression).collect(Collectors.toList());
    TimeBuckets timeBuckets = context.getInitialTimeBuckets();
    ExpressionContext timeTransform = TimeSeriesBucketTransformFunction.create(leafNode.getTimeColumn(),
        leafNode.getTimeUnit(), timeBuckets, leafNode.getOffsetSeconds() == null ? 0 : leafNode.getOffsetSeconds());
    ExpressionContext aggregation = TimeSeriesAggregationFunction.create(context.getLanguage(),
        leafNode.getValueExpression(), timeTransform, timeBuckets, leafNode.getAggInfo());
    Map<String, String> queryOptions = new HashMap<>(leafNode.getQueryOptions());
    queryOptions.put(QueryOptionKey.TIMEOUT_MS, Long.toString(Math.max(0L, context.getRemainingTimeMs())));
    return new QueryContext.Builder()
        .setTableName(leafNode.getTableName())
        .setFilter(filterContext)
        .setGroupByExpressions(groupByExpressions)
        .setSelectExpressions(List.of(aggregation))
        .setQueryOptions(queryOptions)
        .setAliasList(Collections.emptyList())
        .setLimit(leafNode.getLimit())
        .build();
  }

  Map<String, String> getServerQueryRequestMetadataMap(TimeSeriesExecutionContext context) {
    Map<String, String> result = new HashMap<>();
    result.put(MetadataKeys.BROKER_ID, context.getMetadataMap().get(MetadataKeys.BROKER_ID));
    result.put(MetadataKeys.REQUEST_ID, context.getMetadataMap().get(MetadataKeys.REQUEST_ID));
    return result;
  }
}
