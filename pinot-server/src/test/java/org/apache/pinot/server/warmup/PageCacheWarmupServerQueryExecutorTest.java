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
package org.apache.pinot.server.warmup;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * Unit-tests the public entry points of {@link PageCacheWarmupServerQueryExecutor}
 * without executing real queries.  Heavy behaviour is stubbed by spying the
 * {@code warmupTable(..)} private method.
 */
public class PageCacheWarmupServerQueryExecutorTest {

  private InstanceDataManager _instanceMgr;
  private TableDataManager _tableMgr;
  private QueryScheduler _queryScheduler;
  private PageCacheWarmupServerQueryExecutor _warmupServerQueryExecutor;

  @BeforeClass
  public void setUp() {
    _instanceMgr = mock(InstanceDataManager.class);
    _tableMgr = mock(TableDataManager.class);
    _queryScheduler = mock(QueryScheduler.class);

    PinotConfiguration cfg = new PinotConfiguration();
    cfg.setProperty("pinot.server.max.pagecache.warmup.duration.ms", 60_000L);
    cfg.setProperty("pinot.server.max.pagecache.refresh.warmup.qps.rate", 1.0);

    // Stub the static accessor so metrics don’t NPE in unit scope
    MockedStatic<ServerMetrics> metricsStatic = Mockito.mockStatic(ServerMetrics.class);
    metricsStatic.when(ServerMetrics::get).thenReturn(mock(ServerMetrics.class));

    _warmupServerQueryExecutor = Mockito.spy(new PageCacheWarmupServerQueryExecutor(
        _instanceMgr, _queryScheduler, cfg));
  }

  /** Table enables warm‑up; verify computed per‑replica QPS for both restart and refresh. */
  @Test
  public void testWarmupEnabling() throws Exception {
    String table = "testTable";

    Set<String> tables = new HashSet<>(Collections.singletonList(table));
    when(_instanceMgr.getAllTables()).thenReturn(tables);
    when(_instanceMgr.getTableDataManager(table)).thenReturn(_tableMgr);

    // --- build fake table config ---
    TableConfig cfg = mock(TableConfig.class);
    QuotaConfig quota = mock(QuotaConfig.class);
    when(quota.getMaxQPS()).thenReturn(500.0);      // cluster-wide QPS
    when(cfg.getQuotaConfig()).thenReturn(quota);
    when(cfg.getReplication()).thenReturn(5);       // 5 replicas → 100 QPS per replica

    PageCacheWarmupConfig warm = mock(PageCacheWarmupConfig.class);
    when(warm.enableOnRestart()).thenReturn(true);
    when(warm.enableOnRefresh()).thenReturn(true);
    when(cfg.getPageCacheWarmupConfig()).thenReturn(warm);

    when(_tableMgr.getCachedTableConfigAndSchema()).thenReturn(Pair.of(cfg, null));

    // --- spy the heavy private method ---
    ArgumentCaptor<Double> qps = ArgumentCaptor.forClass(Double.class);
    doNothing().when(_warmupServerQueryExecutor)
        .warmupTable(eq(table), eq(warm), isNull(), isNull(), qps.capture());

    _warmupServerQueryExecutor.startWarmupOnRestart();

    // --- verify refresh path uses the same per‑replica QPS (rateLimit=1.0) ---
    _warmupServerQueryExecutor.startWarmupOnRefresh(table, null, null);

    java.util.List<Double> captured = qps.getAllValues();
    // index 0: restart, index 1: refresh
    Assert.assertEquals(captured.size(), 2);
    Assert.assertEquals(captured.get(0), 100.0, 0.0001,
        "Restart QPS should be maxQPS / replication");
    Assert.assertEquals(captured.get(1), 100.0, 0.0001,
        "Refresh QPS should be (maxQPS / replication) * rateLimit (1.0)");
  }

  /**
   * warmupTable() completes within the configured timeout.
   */
  @Test
  public void testRateLimiterCompletesWithinTimeout() throws Exception {
    String table = "testTable";

    // Mock QueryScheduler to return an already‑completed future
    when(_queryScheduler.submit(any())).thenAnswer(inv ->
        com.google.common.util.concurrent.Futures.immediateFuture(new byte[0]));

    // Dummy warm‑up config: 2‑second budget
    PageCacheWarmupConfig warmCfg = mock(PageCacheWarmupConfig.class);
    when(warmCfg.getMaxWarmupDurationSeconds()).thenReturn(2);
    // Enable flags so we can re‑use executeWarmupQueries signature (not checked here)
    when(warmCfg.enableOnRefresh()).thenReturn(true);

    // Wire the table manager and a single segment so warmupTable() finds data
    when(_instanceMgr.getTableDataManager(table)).thenReturn(_tableMgr);

    SegmentDataManager segMgr = mock(SegmentDataManager.class, RETURNS_DEEP_STUBS);
    when(segMgr.getSegment().getSegmentMetadata().getName()).thenReturn("segA");
    when(segMgr.getSegment().getSegmentName()).thenReturn("segA");

    when(_tableMgr.acquireAllSegments()).thenReturn(java.util.Arrays.asList(segMgr));
    doNothing().when(_tableMgr).releaseSegment(any());

    // Build a short query list
    java.util.List<String> queries = java.util.Arrays.asList(
        "SELECT * FROM testTable LIMIT 1",
        "SELECT COUNT(*) FROM testTable"
    );

    double warmupQps = 20.0;  // plenty of headroom (2 queries << 20 QPS)

    // --- Invoke the method directly ---
    long startTimeMs = System.currentTimeMillis();
    _warmupServerQueryExecutor.warmupTable(table, warmCfg, queries, null, warmupQps);

    // Submit should be called once per query
    verify(_queryScheduler, times(queries.size())).submit(any());

    // No timeout‑error metric should be recorded
    verify(ServerMetrics.get(), never())
        .addMeteredGlobalValue(eq(ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS), anyLong());
  }
}
