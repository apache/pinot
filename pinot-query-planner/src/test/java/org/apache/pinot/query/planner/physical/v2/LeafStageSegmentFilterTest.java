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
package org.apache.pinot.query.planner.physical.v2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies that the [LeafStageSegmentFilter] hook is applied to the leaf-stage segment assignment of the multi-stage
/// physical optimizer.
public class LeafStageSegmentFilterTest {
  private static final String QUERY = "SELECT * FROM c";

  private RoutingManager _routingManager;
  private TableCache _tableCache;

  @BeforeClass
  public void setUp() {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : QueryEnvironmentTestBase.TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    _routingManager = factory.buildRoutingManager(null);
    _tableCache = factory.buildTableCache();
  }

  @Test
  public void testDefaultWorkerManagerKeepsAllSegments() {
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, _routingManager);
    assertEquals(leafSegments(plan(workerManager)), Set.of("c1", "c2", "c3"));
  }

  @Test
  public void testFilterNarrowsLeafSegmentsAndKeepsWorkers() {
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, _routingManager) {
      @Override
      public TableScanMetadata filterTableScanSegments(TableScanMetadata metadata,
          Map<String, String> queryOptions) {
        Map<Integer, Map<String, List<String>>> narrowed = new HashMap<>();
        for (Map.Entry<Integer, Map<String, List<String>>> worker : metadata.getWorkedIdToSegmentsMap().entrySet()) {
          Map<String, List<String>> byType = new HashMap<>();
          for (Map.Entry<String, List<String>> segments : worker.getValue().entrySet()) {
            byType.put(segments.getKey(), segments.getValue().stream().filter("c2"::equals).toList());
          }
          narrowed.put(worker.getKey(), byType);
        }
        return new TableScanMetadata(metadata.getScannedTables(), narrowed, metadata.getTableOptions(),
            metadata.getUnavailableSegmentsMap(), metadata.getTimeBoundaryInfo());
      }
    };
    DispatchableSubPlan plan = plan(workerManager);
    assertEquals(leafSegments(plan), Set.of("c2"));
  }

  @Test
  public void testFilterRemovingAllSegmentsKeepsWorkers() {
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, _routingManager) {
      @Override
      public TableScanMetadata filterTableScanSegments(TableScanMetadata metadata, Map<String, String> queryOptions) {
        Map<Integer, Map<String, List<String>>> emptied = new HashMap<>();
        for (Integer workerId : metadata.getWorkedIdToSegmentsMap().keySet()) {
          emptied.put(workerId, Map.of("OFFLINE", List.of()));
        }
        return new TableScanMetadata(metadata.getScannedTables(), emptied, metadata.getTableOptions(),
            metadata.getUnavailableSegmentsMap(), metadata.getTimeBoundaryInfo());
      }
    };
    assertEquals(leafSegments(plan(workerManager)), Set.of());
  }

  @Test
  public void testFilterDroppingAWorkerIsRejected() {
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, _routingManager) {
      @Override
      public TableScanMetadata filterTableScanSegments(TableScanMetadata metadata, Map<String, String> queryOptions) {
        return new TableScanMetadata(metadata.getScannedTables(), Map.of(), metadata.getTableOptions(),
            metadata.getUnavailableSegmentsMap(), metadata.getTimeBoundaryInfo());
      }
    };
    QueryException exception = expectThrows(QueryException.class, () -> plan(workerManager));
    assertTrue(exception.getMessage().contains("must keep the workers of the table scan c: expected workers [0, 1]"),
        exception.getMessage());
  }

  private DispatchableSubPlan plan(WorkerManager workerManager) {
    QueryEnvironment queryEnvironment = new QueryEnvironment(
        QueryEnvironment.configBuilder()
            .requestId(-1L)
            .database(CommonConstants.DEFAULT_DATABASE)
            .tableCache(_tableCache)
            .workerManager(workerManager)
            .defaultUsePhysicalOptimizer(true)
            .build());
    return queryEnvironment.planQuery(QUERY);
  }

  private static Set<String> leafSegments(DispatchableSubPlan plan) {
    Set<String> segments = new HashSet<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      for (Map<String, List<String>> byType : fragment.getWorkerIdToSegmentsMap().values()) {
        byType.values().forEach(segments::addAll);
      }
    }
    return segments;
  }
}
