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
package org.apache.pinot.tsdb.planner;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteProvider;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.planner.physical.TableScanVisitor;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesRoutingCustomizer;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.TimeSeriesMetadata;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSeriesQueryEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesQueryEnvironment.class);
  private final RoutingManager _routingManager;
  private final TableCache _tableCache;
  private final TimeSeriesMetadata _metadataProvider;
  private final TimeSeriesRoutingCustomizer _routingCustomizer;
  private final Map<String, TimeSeriesLogicalPlanner> _plannerMap = new HashMap<>();

  public TimeSeriesQueryEnvironment(PinotConfiguration config, RoutingManager routingManager, TableCache tableCache,
      TimeSeriesRoutingCustomizer routingCustomizer) {
    _routingManager = routingManager;
    _tableCache = tableCache;
    _metadataProvider = new TimeSeriesTableMetadataProvider(_tableCache);
    _routingCustomizer = routingCustomizer;
  }

  public void init(PinotConfiguration config) {
    String[] languages = config.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "")
        .split(",");
    LOGGER.info("Found {} configured time series languages. List: {}", languages.length, languages);
    for (String language : languages) {
      try {
        TimeSeriesLogicalPlanner planner = buildLogicalPlanner(language, config);
        _plannerMap.put(language, planner);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate logical planner for language: " + language, e);
      }
    }
    // TODO(timeseries): Add support for logical tables in the future.
    TableScanVisitor.INSTANCE.init(_routingManager, new ImplicitHybridTableRouteProvider(), _tableCache);
  }

  public static TimeSeriesLogicalPlanner buildLogicalPlanner(String language, PinotConfiguration config)
      throws RuntimeException {
    String configPrefix = PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(language);
    String klassName = config.getProperty(configPrefix);
    Preconditions.checkNotNull(klassName, "Logical planner class not found for language: " + language);
    // Create the planner with empty constructor
    try {
      Class<?> klass = TimeSeriesQueryEnvironment.class.getClassLoader().loadClass(klassName);
      Constructor<?> constructor = klass.getConstructor();
      TimeSeriesLogicalPlanner planner = (TimeSeriesLogicalPlanner) constructor.newInstance();
      planner.init(config.subset(configPrefix));
      return planner;
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate logical planner for language: " + language, e);
    }
  }

  public TimeSeriesLogicalPlanResult buildLogicalPlan(RangeTimeSeriesRequest request) {
    Preconditions.checkState(_plannerMap.containsKey(request.getLanguage()),
        "No logical planner found for engine: %s. Available: %s", request.getLanguage(),
        _plannerMap.keySet());
    return _plannerMap.get(request.getLanguage()).plan(request, _metadataProvider);
  }

  /// @param tableAuthorizer authorizes the query's tables, named with their type; it runs before any routing, so a
  ///     caller who may not query a table gets its error before a [TimeSeriesRoutingCustomizer] does any work
  public TimeSeriesDispatchablePlan buildPhysicalPlan(RangeTimeSeriesRequest timeSeriesRequest,
      RequestContext requestContext, TimeSeriesLogicalPlanResult logicalPlan, Consumer<List<String>> tableAuthorizer) {
    // Step-0: Add table type info to the logical plan, and authorize the tables before routing them.
    logicalPlan = new TimeSeriesLogicalPlanResult(TableScanVisitor.INSTANCE.addTableTypeInfoToPlan(
      logicalPlan.getPlanNode(), requestContext), logicalPlan.getTimeBuckets());
    tableAuthorizer.accept(leafTableNames(logicalPlan.getPlanNode()));
    // Step-1: Assign segments to servers for each leaf node.
    TableScanVisitor.Context scanVisitorContext =
        TableScanVisitor.createContext(requestContext.getRequestId(), timeSeriesRequest.getQueryOptions(),
            _routingCustomizer);
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(logicalPlan.getPlanNode(), logicalPlan.getTimeBuckets(),
        scanVisitorContext);
    // Step-1b: Send the query options the routing customizer added with every leaf.
    logicalPlan = new TimeSeriesLogicalPlanResult(TableScanVisitor.INSTANCE.addQueryOptionsToPlan(
        logicalPlan.getPlanNode(), scanVisitorContext.getRoutingQueryOptions()), logicalPlan.getTimeBuckets());
    List<TimeSeriesQueryServerInstance> serverInstances = scanVisitorContext.getQueryServers();
    Map<String, Map<String, List<String>>> leafIdToSegmentsByInstanceId =
        scanVisitorContext.getLeafIdToSegmentsByInstanceId();
    // Step-2: Create plan fragments. One server runs the whole plan only when it holds segments for every leaf: a
    // leaf it holds none for must not run there, since its table may not be on that server, and must read nothing.
    boolean wholePlanOnOneServer = serverInstances.size() == 1 && leafIdToSegmentsByInstanceId.get(
        serverInstances.get(0).getInstanceId()).keySet().containsAll(leafIds(logicalPlan.getPlanNode()));
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(
        logicalPlan.getPlanNode(), wholePlanOnOneServer);
    // Step-3: Compute number of servers each exchange node will receive data from.
    Map<String, Integer> numServersForExchangePlanNode = computeNumServersForExchangePlanNode(wholePlanOnOneServer,
        fragments, leafIdToSegmentsByInstanceId);
    return new TimeSeriesDispatchablePlan(timeSeriesRequest.getLanguage(), serverInstances, fragments.get(0),
        fragments.subList(1, fragments.size()), wholePlanOnOneServer, logicalPlan.getTimeBuckets(),
        leafIdToSegmentsByInstanceId, scanVisitorContext.getLeafIdToOptionalSegmentsByInstanceId(),
        numServersForExchangePlanNode, scanVisitorContext.getTableNames());
  }

  /// The distinct tables the plan's leaves scan, in plan order.
  private static List<String> leafTableNames(BaseTimeSeriesPlanNode planNode) {
    Set<String> tableNames = new LinkedHashSet<>();
    collectLeafTableNames(planNode, tableNames);
    return new ArrayList<>(tableNames);
  }

  private static Set<String> leafIds(BaseTimeSeriesPlanNode planNode) {
    Set<String> leafIds = new HashSet<>();
    collectLeafIds(planNode, leafIds);
    return leafIds;
  }

  private static void collectLeafIds(BaseTimeSeriesPlanNode planNode, Set<String> leafIds) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      leafIds.add(planNode.getId());
    }
    for (BaseTimeSeriesPlanNode input : planNode.getInputs()) {
      collectLeafIds(input, leafIds);
    }
  }

  private static void collectLeafTableNames(BaseTimeSeriesPlanNode planNode, Set<String> tableNames) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      tableNames.add(((LeafTimeSeriesPlanNode) planNode).getTableName());
    }
    for (BaseTimeSeriesPlanNode input : planNode.getInputs()) {
      collectLeafTableNames(input, tableNames);
    }
  }

  private Map<String, Integer> computeNumServersForExchangePlanNode(boolean wholePlanOnOneServer,
      List<BaseTimeSeriesPlanNode> planNodes, Map<String, Map<String, List<String>>> leafIdToSegmentsByInstanceId) {
    if (wholePlanOnOneServer) {
      // For single-server case, the broker fragment consists only of the TimeSeriesExchangeNode.
      return Map.of(planNodes.get(0).getId(), 1);
    }
    // Otherwise each leaf is its own fragment. The leafIdToSegmentsByInstanceId map already has the information we
    // need, but we just need to restructure it so that we can get number of servers by planId.
    Map<String, Set<String>> planIdToServers = new HashMap<>();
    for (var entry : leafIdToSegmentsByInstanceId.entrySet()) {
      String instanceId = entry.getKey();
      for (var innerEntry : entry.getValue().entrySet()) {
        String planId = innerEntry.getKey();
        planIdToServers.computeIfAbsent(planId, (x) -> new HashSet<>()).add(instanceId);
      }
    }
    Map<String, Integer> result = new HashMap<>();
    // A leaf no server holds segments for still gets an exchange; it receives from no server and yields no data, as a
    // range with no data does. With no server at all, every leaf is such a leaf.
    for (BaseTimeSeriesPlanNode serverFragment : planNodes.subList(1, planNodes.size())) {
      result.put(serverFragment.getId(), 0);
    }
    for (var entry : planIdToServers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().size());
    }
    return result;
  }
}
