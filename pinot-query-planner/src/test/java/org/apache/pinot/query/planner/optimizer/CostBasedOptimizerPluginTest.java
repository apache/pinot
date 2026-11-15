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
package org.apache.pinot.query.planner.optimizer;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// End-to-end proof that a [QueryOptimizerProvider] plugin with a VolcanoPlanner
/// can plan a query through the whole pipeline (logical optimize -> trait program
/// -> plan-node conversion).
public class CostBasedOptimizerPluginTest extends QueryEnvironmentTestBase {

  @Test
  public void testNoopOptimizerPlansQuery() {
    NoopOptimizerProvider.FIND_BEST_EXP_CALLS.set(0);
    QueryEnvironment queryEnvironment = buildQueryEnvironment();

    DispatchableSubPlan plan =
        queryEnvironment.planQuery("SET queryOptimizer='" + NoopOptimizerProvider.NAME + "'; SELECT * FROM a");

    assertNotNull(plan);
    assertFalse(plan.getQueryStageMap().isEmpty());
    assertTrue(NoopOptimizerProvider.FIND_BEST_EXP_CALLS.get() > 0, "plugin's planner was never invoked");
  }

  private static QueryEnvironment buildQueryEnvironment() {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    QueryEnvironment.Config config = QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .workerManager(new WorkerManager("Broker_localhost", "localhost", 0, routingManager))
        .optimizerRegistry(new QueryOptimizerRegistry(List.of(new NoopOptimizerProvider())))
        .build();
    return new QueryEnvironment(config);
  }
}
