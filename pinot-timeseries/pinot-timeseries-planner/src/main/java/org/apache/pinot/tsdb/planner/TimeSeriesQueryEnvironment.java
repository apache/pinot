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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.planner.physical.TableScanVisitor;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanner;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeSeriesQueryEnvironment {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesQueryEnvironment.class);
  private final RoutingManager _routingManager;
  private final TableCache _tableCache;
  private final Map<String, TimeSeriesLogicalPlanner> _plannerMap = new HashMap<>();

  public TimeSeriesQueryEnvironment(PinotConfiguration config, RoutingManager routingManager, TableCache tableCache) {
    _routingManager = routingManager;
    _tableCache = tableCache;
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
    return _plannerMap.get(request.getLanguage()).plan(request);
  }

  public TimeSeriesDispatchablePlan buildPhysicalPlan(RangeTimeSeriesRequest timeSeriesRequest,
      RequestContext requestContext, TimeSeriesLogicalPlanResult logicalPlan) {
    // Step-1: Find tables in the query.
    final Set<String> tableNames = new HashSet<>();
    findTableNames(logicalPlan.getPlanNode(), tableNames::add);
    Preconditions.checkState(tableNames.size() == 1,
        "Expected exactly one table name in the logical plan, got: %s",
        tableNames);
    String tableName = tableNames.iterator().next();
    // Step-2: Compute routing table assuming all segments are selected. This is to perform the check to reject tables
    //         that span across multiple servers.
    RoutingTable routingTable = _routingManager.getRoutingTable(compileBrokerRequest(tableName),
        requestContext.getRequestId());
    Preconditions.checkState(routingTable != null,
        "Failed to get routing table for table: %s", tableName);
    Preconditions.checkState(routingTable.getServerInstanceToSegmentsMap().size() == 1,
        "Only support routing to a single server. Computed: %s",
        routingTable.getServerInstanceToSegmentsMap().size());
    var entry = routingTable.getServerInstanceToSegmentsMap().entrySet().iterator().next();
    ServerInstance serverInstance = entry.getKey();
    // Step-3: Assign segments to the leaf plan nodes.
    TableScanVisitor.Context scanVisitorContext = TableScanVisitor.createContext(requestContext.getRequestId());
    TableScanVisitor.INSTANCE.assignSegmentsToPlan(logicalPlan.getPlanNode(), logicalPlan.getTimeBuckets(),
        scanVisitorContext);
    return new TimeSeriesDispatchablePlan(timeSeriesRequest.getLanguage(),
        new TimeSeriesQueryServerInstance(serverInstance),
        TimeSeriesPlanSerde.serialize(logicalPlan.getPlanNode()), logicalPlan.getTimeBuckets(),
        scanVisitorContext.getPlanIdToSegmentMap());
  }

  public static void findTableNames(BaseTimeSeriesPlanNode planNode, Consumer<String> tableNameConsumer) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode scanNode = (LeafTimeSeriesPlanNode) planNode;
      tableNameConsumer.accept(scanNode.getTableName());
      return;
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      findTableNames(childNode, tableNameConsumer);
    }
  }

  private BrokerRequest compileBrokerRequest(String tableName) {
    DataSource dataSource = new DataSource();
    dataSource.setTableName(tableName);
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(dataSource);
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableName);
    BrokerRequest dummyRequest = new BrokerRequest();
    dummyRequest.setPinotQuery(pinotQuery);
    dummyRequest.setQuerySource(querySource);
    return dummyRequest;
  }
}
