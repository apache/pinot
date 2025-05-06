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
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.planner.physical.TableScanVisitor;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.TimeSeriesMetadata;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSeriesQueryEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesQueryEnvironment.class);
  private final RoutingManager _routingManager;
  private final TableCache _tableCache;
  private final TimeSeriesMetadata _metadataProvider;
  private final Map<String, TimeSeriesLogicalPlanner> _plannerMap = new HashMap<>();

  public TimeSeriesQueryEnvironment(PinotConfiguration config, RoutingManager routingManager, TableCache tableCache) {
    _routingManager = routingManager;
    _tableCache = tableCache;
    _metadataProvider = new TimeSeriesTableMetadataProvider(_tableCache);
  }

  public void init(PinotConfiguration config) {
    String[] languages = config.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "")
        .split(",");
    LOGGER.info("Found {} configured time series languages. List: {}", languages.length, languages);
    for (String language : languages) {
      String configPrefix = PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(language);
      String klassName =
          config.getProperty(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey(language));
      Preconditions.checkNotNull(klassName, "Logical planner class not found for language: " + language);
      // Create the planner with empty constructor
      try {
        Class<?> klass = TimeSeriesQueryEnvironment.class.getClassLoader().loadClass(klassName);
        Constructor<?> constructor = klass.getConstructor();
        TimeSeriesLogicalPlanner planner = (TimeSeriesLogicalPlanner) constructor.newInstance();
        planner.init(config.subset(configPrefix));
        _plannerMap.put(language, planner);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate logical planner for language: " + language, e);
      }
    }
    TableScanVisitor.INSTANCE.init(_routingManager);
  }

  public TimeSeriesLogicalPlanResult buildLogicalPlan(RangeTimeSeriesRequest request) {
    Preconditions.checkState(_plannerMap.containsKey(request.getLanguage()),
        "No logical planner found for engine: %s. Available: %s", request.getLanguage(),
        _plannerMap.keySet());
    return _plannerMap.get(request.getLanguage()).plan(request, _metadataProvider);
  }

  public TimeSeriesDispatchablePlan buildPhysicalPlan(RangeTimeSeriesRequest timeSeriesRequest,
      RequestContext requestContext, TimeSeriesLogicalPlanResult logicalPlan) {
    // Step-1: Assign segments to servers for each leaf node.
    TableScanVisitor.Context scanVisitorContext = TableScanVisitor.createContext(requestContext.getRequestId());
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(logicalPlan.getPlanNode(), logicalPlan.getTimeBuckets(),
        scanVisitorContext);
    List<TimeSeriesQueryServerInstance> serverInstances = scanVisitorContext.getQueryServers();
    // Step-2: Create plan fragments.
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(
        logicalPlan.getPlanNode(), serverInstances.size() == 1);
    // Step-3: Compute number of servers each exchange node will receive data from.
    Map<String, Integer> numServersForExchangePlanNode = computeNumServersForExchangePlanNode(serverInstances,
        fragments, scanVisitorContext.getLeafIdToSegmentsByInstanceId());
    return new TimeSeriesDispatchablePlan(timeSeriesRequest.getLanguage(), serverInstances, fragments.get(0),
        fragments.subList(1, fragments.size()), logicalPlan.getTimeBuckets(),
        scanVisitorContext.getLeafIdToSegmentsByInstanceId(), numServersForExchangePlanNode);
  }

  private Map<String, Integer> computeNumServersForExchangePlanNode(List<TimeSeriesQueryServerInstance> serverInstances,
      List<BaseTimeSeriesPlanNode> planNodes, Map<String, Map<String, List<String>>> leafIdToSegmentsByInstanceId) {
    // TODO(timeseries): Handle this gracefully and return an empty block.
    Preconditions.checkState(!serverInstances.isEmpty(), "No servers selected for the query");
    if (serverInstances.size() == 1) {
      // For single-server case, the broker fragment consists only of the TimeSeriesExchangeNode.
      return ImmutableMap.of(planNodes.get(0).getId(), 1);
    }
    // For the multi-server case, the leafIdToSegmentsByInstanceId map already has the information we need, but we
    // just need to restructure it so that we can get number of servers by planId.
    Map<String, Set<String>> planIdToServers = new HashMap<>();
    for (var entry : leafIdToSegmentsByInstanceId.entrySet()) {
      String instanceId = entry.getKey();
      for (var innerEntry : entry.getValue().entrySet()) {
        String planId = innerEntry.getKey();
        planIdToServers.computeIfAbsent(planId, (x) -> new HashSet<>()).add(instanceId);
      }
    }
    Map<String, Integer> result = new HashMap<>();
    for (var entry : planIdToServers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().size());
    }
    return result;
  }
}
