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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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

  @Test
  public void testUnknownOptimizerFailsValidation() {
    QueryEnvironment queryEnvironment = buildQueryEnvironment();

    QueryException e = expectThrows(QueryException.class,
        () -> queryEnvironment.planQuery("SET queryOptimizer='doesNotExist'; SELECT * FROM a"));

    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    assertTrue(e.getMessage().contains("doesNotExist"), e.getMessage());
    assertTrue(e.getMessage().contains(NoopOptimizerProvider.NAME), e.getMessage());
  }

  @Test
  public void testPluginMetadataProvidersAreConsultedAndThreadLocalRestored() {
    JaninoRelMetadataProvider before = RelMetadataQueryBase.THREAD_PROVIDERS.get();
    ScanRowCountHandler handler = new ScanRowCountHandler(false);
    QueryEnvironment queryEnvironment = buildQueryEnvironment(new NoopOptimizerProvider(List.of(handler.source())));

    DispatchableSubPlan plan = queryEnvironment.planQuery(
        "SET queryOptimizer='" + NoopOptimizerProvider.NAME + "'; SELECT * FROM a JOIN b ON a.col1 = b.col2");

    assertNotNull(plan);
    assertTrue(handler._calls.get() > 0, "plugin's row-count handler was never consulted");
    // Compare by value: every RelOptCluster constructor installs a fresh JaninoRelMetadataProvider
    // wrapping the defaults, which equals (but is not the same as) the earlier instance.
    assertEquals(RelMetadataQueryBase.THREAD_PROVIDERS.get(), before,
        "planning must not leave the plugin's metadata provider on the thread");
  }

  @Test
  public void testThreadLocalRestoredWhenPlanningFails() {
    JaninoRelMetadataProvider before = RelMetadataQueryBase.THREAD_PROVIDERS.get();
    ScanRowCountHandler handler = new ScanRowCountHandler(true);
    QueryEnvironment queryEnvironment = buildQueryEnvironment(new NoopOptimizerProvider(List.of(handler.source())));

    QueryException e = expectThrows(QueryException.class, () -> queryEnvironment.planQuery(
        "SET queryOptimizer='" + NoopOptimizerProvider.NAME + "'; SELECT * FROM a JOIN b ON a.col1 = b.col2"));

    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_PLANNING);
    assertTrue(handler._calls.get() > 0, "failure should come from the plugin's handler");
    assertEquals(RelMetadataQueryBase.THREAD_PROVIDERS.get(), before,
        "a failed compile must still restore the thread's metadata provider");
  }

  /// Row-count handler for table scans only; Calcite's defaults answer for every other node.
  /// Public so Calcite's Janino-generated dispatch code can call it.
  public static final class ScanRowCountHandler implements MetadataHandler<BuiltInMetadata.RowCount> {
    private final AtomicInteger _calls = new AtomicInteger();
    private final boolean _fail;

    ScanRowCountHandler(boolean fail) {
      _fail = fail;
    }

    RelMetadataProvider source() {
      return ReflectiveRelMetadataProvider.reflectiveSource(this, BuiltInMetadata.RowCount.Handler.class);
    }

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
      return BuiltInMetadata.RowCount.DEF;
    }

    public Double getRowCount(TableScan scan, RelMetadataQuery mq) {
      _calls.incrementAndGet();
      if (_fail) {
        throw new IllegalStateException("injected statistics failure");
      }
      return 42d;
    }
  }

  private static QueryEnvironment buildQueryEnvironment() {
    return buildQueryEnvironment(new NoopOptimizerProvider());
  }

  private static QueryEnvironment buildQueryEnvironment(NoopOptimizerProvider provider) {
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
        .optimizerRegistry(new QueryOptimizerRegistry(List.of(provider)))
        .build();
    return new QueryEnvironment(config);
  }
}
