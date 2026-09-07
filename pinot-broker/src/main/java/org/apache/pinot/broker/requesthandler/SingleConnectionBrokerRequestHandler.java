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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.ImplicitHybridTableRouteInfo;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.materializedview.handler.MaterializedViewHandler;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `SingleConnectionBrokerRequestHandler` class is a thread-safe broker request handler using a single
/// connection per server to route the queries.
@ThreadSafe
public class SingleConnectionBrokerRequestHandler extends BaseSingleStageBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleConnectionBrokerRequestHandler.class);
  /// Per-probe cap, also bounded by whatever remains of the warmup budget.
  private static final long WARMUP_PROBE_TIMEOUT_MS = 5_000L;
  /// Backoff after an unproductive probe round (nothing routable / every probe failed), so a not-yet-ready
  /// cluster cannot busy-spin the warmup thread for the whole budget during startup.
  private static final long WARMUP_FAILURE_BACKOFF_MS = 100L;

  protected final BrokerReduceService _brokerReduceService;
  protected final QueryRouter _queryRouter;
  protected final FailureDetector _failureDetector;

  /// Legacy constructor without an MV handler — see [BaseSingleStageBrokerRequestHandler]'s
  /// legacy ctor for the rationale.  Delegates with `materializedViewHandler = null`.
  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      NettyConfig nettyConfig, TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager,
      FailureDetector failureDetector, ThreadAccountant threadAccountant,
      MultiClusterRoutingContext multiClusterRoutingContext) {
    this(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        nettyConfig, tlsConfig, serverRoutingStatsManager, failureDetector, threadAccountant,
        multiClusterRoutingContext, null);
  }

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      NettyConfig nettyConfig, TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager,
      FailureDetector failureDetector, ThreadAccountant threadAccountant,
      MultiClusterRoutingContext multiClusterRoutingContext,
      @Nullable MaterializedViewHandler materializedViewHandler) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant, multiClusterRoutingContext, materializedViewHandler);
    _brokerReduceService = new BrokerReduceService(_config);
    _queryRouter = new QueryRouter(_brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant);
    _failureDetector = failureDetector;
    _failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void shutDown() {
    super.shutDown();
    _queryRouter.shutDown();
    _brokerReduceService.shutDown();
  }

  /// Warms the scatter-gather path before readiness is granted: repeatedly runs real probe queries until
  /// they have run enough times (the `minIterations` depth floor) or the budget expires. Broker-to-server
  /// channels are opened lazily by the probes themselves.
  ///
  /// Probes go through [QueryRouter] rather than [#handleRequest], deliberately bypassing access control,
  /// query quota and the query log -- warmup then needs no synthetic identity and pollutes no
  /// customer-facing surface. Never throws, and always returns within the configured budget.
  @Override
  public boolean warmUp(BrokerWarmupConfig config, long deadlineMs) {
    // Stage 1: local, no-network warmup of the compile + response-serialization paths, run before the
    // network probe so they are warm even if no server is reachable.
    warmUpLocal();
    int concurrency = Math.max(1, config.concurrency());
    ExecutorService probePool = Executors.newFixedThreadPool(concurrency,
        new ThreadFactoryBuilder().setNameFormat("broker-warmup-probe-%d").setDaemon(true).build());
    try {
      return config.hasCustomQuery() ? warmUpWithCustomQuery(config, deadlineMs, probePool, concurrency)
          : warmUpWithAutoSelect(config, deadlineMs, probePool, concurrency);
    } catch (Exception e) {
      LOGGER.warn("Broker warmup failed; proceeding without it", e);
      return false;
    } finally {
      probePool.shutdownNow();
    }
  }

  /// Stage 1: local, no-network warmup. The network probe goes through [QueryRouter] and stops at the
  /// gathered DataTables, so it never exercises the SQL compile path's cold start nor the response build +
  /// JSON serialization the first real query pays. This compiles a throwaway query and serializes a small
  /// synthetic [BrokerResponseNative], warming both. Never throws.
  private void warmUpLocal() {
    try {
      CalciteSqlCompiler.compileToBrokerRequest("SELECT 1");
      BrokerResponseNative response = new BrokerResponseNative();
      response.setResultTable(new ResultTable(
          new DataSchema(new String[]{"warmup"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG}),
          Collections.singletonList(new Object[]{1L})));
      response.setNumDocsScanned(1);
      response.toJsonString();
    } catch (Exception e) {
      LOGGER.debug("Local (stage 1) warmup failed; continuing", e);
    }
  }

  /// Fires the given probe tasks concurrently on the shared pool and returns the latencies (ms) of the
  /// probes that completed successfully. Empty means the whole round was unproductive (nothing routable
  /// yet / all failed); a failed probe simply does not contribute.
  private List<Long> runConcurrentRound(List<Callable<Long>> tasks, ExecutorService pool) {
    List<Future<Long>> futures = new ArrayList<>(tasks.size());
    for (Callable<Long> task : tasks) {
      futures.add(pool.submit(task));
    }
    List<Long> latencies = new ArrayList<>(futures.size());
    for (Future<Long> future : futures) {
      try {
        Long elapsedMs = future.get(WARMUP_PROBE_TIMEOUT_MS + 1000L, TimeUnit.MILLISECONDS);
        if (elapsedMs != null && elapsedMs >= 0) {
          latencies.add(elapsedMs);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return latencies;
      } catch (Exception e) {
        // Individual probe failed/timed out; it just does not count toward the round. Cancel it so a probe
        // that outlived the round's get()-timeout does not keep running behind the next round (no-op if it
        // already completed).
        future.cancel(true);
      }
    }
    return latencies;
  }

  /// Sleeps [#WARMUP_FAILURE_BACKOFF_MS] after an unproductive probe round. Returns `false` if interrupted
  /// (shutdown) so the caller stops promptly and readiness opens.
  private boolean warmupBackoff() {
    try {
      Thread.sleep(WARMUP_FAILURE_BACKOFF_MS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /// Default probe path: probe `SELECT * FROM "<t>" LIMIT 1` over the configured tables or a greedy
  /// set-cover of tables covering every routable server. Exits once at least `minIterations` probes have
  /// completed successfully -- a depth floor that guarantees enough invocations to drive the query path's
  /// JIT to its top tier -- OR the budget expires. A latency target is deliberately not used: a trivial
  /// probe reaches low latency after a handful of iterations while the code is still only partially
  /// compiled, so latency is a false early-exit signal.
  private boolean warmUpWithAutoSelect(BrokerWarmupConfig config, long deadlineMs, ExecutorService pool,
      int concurrency) {
    long successfulProbes = 0;
    int rounds = 0;
    // Select the probe tables once and reuse them across rounds. Only re-select while the result is empty
    // (routing not populated yet); recomputing the greedy set-cover every round would repeat O(tables) work
    // up to minIterations times on a large-table tenant, for a result that does not change once non-empty.
    List<String> tables = List.of();
    boolean coverageReported = false;
    // Monotonic across rounds so the round-robin actually advances through every covered table even at
    // concurrency 1; resetting per round would probe only the first `concurrency` tables forever.
    int probeSeq = 0;
    while (System.currentTimeMillis() < deadlineMs && !Thread.currentThread().isInterrupted()) {
      if (tables.isEmpty()) {
        tables = config.hasCustomTables() ? filterRoutableTables(_routingManager, config.tables())
            : selectProbeTables(_routingManager, config.maxTables());
      }
      if (tables.isEmpty()) {
        // Converged but nothing routable yet: back off and retry rather than declaring the broker warm --
        // an empty routing table here would otherwise make the gate a no-op precisely on a cold broker.
        if (!warmupBackoff()) {
          return false;
        }
        continue;
      }
      if (!coverageReported) {
        // Report once, now that the probe tables are settled, whether they route to every server.
        coverageReported = true;
        reportServerCoverage(_routingManager, _brokerMetrics, config.hasCustomTables(), tables);
      }
      long remainingMs = deadlineMs - System.currentTimeMillis();
      if (remainingMs <= 0) {
        break;
      }
      rounds++;
      long probeTimeoutMs = Math.min(remainingMs, WARMUP_PROBE_TIMEOUT_MS);
      // A batch of `concurrency` probes, round-robin over the covered tables (advancing across rounds via
      // probeSeq): exercises every server (coverage) AND real concurrency (contention) at once.
      List<Callable<Long>> tasks = new ArrayList<>(concurrency);
      for (String tableNameWithType : roundRobinBatch(tables, probeSeq, concurrency)) {
        tasks.add(() -> probe(compileProbe(tableNameWithType), probeTimeoutMs));
      }
      probeSeq += concurrency;
      List<Long> latencies = runConcurrentRound(tasks, pool);
      if (latencies.isEmpty()) {
        if (!warmupBackoff()) {
          return false;
        }
        continue;
      }
      successfulProbes += latencies.size();
      if (successfulProbes >= config.minIterations()) {
        LOGGER.info("Broker warmup completed after {} round(s) at concurrency {}; {} probes (floor {})", rounds,
            concurrency, successfulProbes, config.minIterations());
        return true;
      }
    }
    LOGGER.warn("Broker warmup budget expired after {} round(s) at concurrency {}; {} probes (floor {}). Proceeding "
        + "to serve traffic.", rounds, concurrency, successfulProbes, config.minIterations());
    return false;
  }

  /// Custom-query probe path: run the operator-supplied queries repeatedly, all of them every round, and
  /// exit on the same `minIterations` probe-count floor as the default probe, or the budget. A probe-count
  /// floor is a cost-independent warm signal; latency plateauing is not (a trivial query reaches a stable
  /// latency while still cold), so it is deliberately not used here. An expensive query that cannot reach
  /// the floor within the budget exits on the budget, having warmed as much as the budget allowed.
  private boolean warmUpWithCustomQuery(BrokerWarmupConfig config, long deadlineMs, ExecutorService pool,
      int concurrency) {
    // Validate-compile each configured query once so a bad one fails fast (and never busy-spins), keeping
    // only those that compile. Each probe below compiles its own fresh request so nothing mutable is shared
    // across the concurrent tasks.
    List<String> queries = new ArrayList<>(config.queries().size());
    for (String query : config.queries()) {
      try {
        CalciteSqlCompiler.compileToBrokerRequest(query);
        queries.add(query);
      } catch (Exception e) {
        LOGGER.warn("Broker warmup query failed to compile; skipping it: {}", query, e);
      }
    }
    if (queries.isEmpty()) {
      LOGGER.warn("Broker warmup: no configured query compiled; skipping warmup");
      return false;
    }
    long successfulProbes = 0;
    int rounds = 0;
    while (System.currentTimeMillis() < deadlineMs && !Thread.currentThread().isInterrupted()) {
      long remainingMs = deadlineMs - System.currentTimeMillis();
      if (remainingMs <= 0) {
        break;
      }
      rounds++;
      long probeTimeoutMs = Math.min(remainingMs, WARMUP_PROBE_TIMEOUT_MS);
      // Run every configured query each round (each `concurrency` times, to warm the concurrency step too).
      List<Callable<Long>> tasks = new ArrayList<>(queries.size() * concurrency);
      for (String query : queries) {
        for (int i = 0; i < concurrency; i++) {
          tasks.add(() -> probe(CalciteSqlCompiler.compileToBrokerRequest(query), probeTimeoutMs));
        }
      }
      List<Long> latencies = runConcurrentRound(tasks, pool);
      if (latencies.isEmpty()) {
        if (!warmupBackoff()) {
          return false;
        }
        continue;
      }
      successfulProbes += latencies.size();
      // Same depth floor as the default probe: a probe-count floor is a cost-independent warm signal (it
      // drives the query path's JIT to its top tier), whereas latency plateauing is not -- a trivial query
      // reaches a stable latency while still cold. An expensive query that cannot reach the floor within the
      // budget simply exits on the budget, having warmed as much as the budget allowed.
      if (successfulProbes >= config.minIterations()) {
        LOGGER.info("Broker warmup (custom query) completed after {} round(s) at concurrency {}; {} probes "
            + "(floor {})", rounds, concurrency, successfulProbes, config.minIterations());
        return true;
      }
    }
    LOGGER.warn("Broker warmup (custom query) budget expired after {} round(s) at concurrency {}; {} probes "
        + "(floor {}). Proceeding to serve traffic.", rounds, concurrency, successfulProbes, config.minIterations());
    return false;
  }

  /// Compiles the default probe query for a single physical table.
  private BrokerRequest compileProbe(String tableNameWithType) {
    return CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\" LIMIT 1");
  }

  /// Issues one probe query through [QueryRouter] and returns its wall-clock duration in ms, or -1 on
  /// failure. Builds the route the same way the normal path does, minus auth/quota/logging.
  private long probe(BrokerRequest brokerRequest, long timeoutMs) {
    String tableName = brokerRequest.getQuerySource().getTableName();
    try {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      long requestId = _requestIdGenerator.get();
      TableRouteInfo routeInfo =
          _implicitHybridTableRouteProvider.getTableRouteInfo(rawTableName, _tableCache, _routingManager);
      if (!(routeInfo instanceof ImplicitHybridTableRouteInfo) || !routeInfo.isExists()) {
        return -1;
      }
      // A type-suffixed table (the default probe) targets exactly one type; a raw name (possible with a
      // custom query) targets whichever types actually exist. Each leg's request must carry the
      // type-suffixed table name -- that is what routing resolves against -- so build a per-leg request
      // from the base query (as the normal path does) rather than reusing the raw-named request, which
      // would route to no server and yield an empty request map.
      BrokerRequest offlineBrokerRequest = shouldRouteOffline(tableType, routeInfo)
          ? typedRequest(brokerRequest, routeInfo.getOfflineTableName()) : null;
      BrokerRequest realtimeBrokerRequest = shouldRouteRealtime(tableType, routeInfo)
          ? typedRequest(brokerRequest, routeInfo.getRealtimeTableName()) : null;
      if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
        // Neither leg is routable right now (e.g. a hybrid table probed via its offline name before the
        // time boundary exists). submitQuery asserts at least one leg is non-null, so skip cleanly rather
        // than build an empty route -- the table is retried on the next round once its leg appears.
        return -1;
      }
      ImplicitHybridTableRouteInfo hybridRouteInfo = (ImplicitHybridTableRouteInfo) routeInfo;
      hybridRouteInfo.setOfflineBrokerRequest(offlineBrokerRequest);
      hybridRouteInfo.setRealtimeBrokerRequest(realtimeBrokerRequest);
      _implicitHybridTableRouteProvider.calculateRoutes(routeInfo, _routingManager, offlineBrokerRequest,
          realtimeBrokerRequest, requestId);
      if (!routeInfo.isRouteExists()) {
        return -1;
      }
      long startMs = System.currentTimeMillis();
      // Scatter/gather (+ DataTable deserialize on receive) is the dominant cost and does not need a
      // QueryThreadContext; run it directly so its warmth always counts toward the probe latency.
      AsyncQueryResponse response = _queryRouter.submitQuery(requestId, rawTableName, routeInfo, timeoutMs);
      Map<ServerRoutingInstance, ServerResponse> finalResponses = response.getFinalResponses();
      // Best-effort: also warm the broker reduce path (result discarded) so the first real query of this
      // shape does not pay it. Isolated in its own try -- reduceOnDataTable requires a QueryThreadContext
      // and could fail for edge cases; a reduce failure must never fail the probe, since the
      // scatter/gather/deserialize warmth has already happened.
      try (QueryThreadContext ignore = QueryThreadContext.open(
          new QueryExecutionContext(QueryExecutionContext.QueryType.SSE, requestId, Long.toString(requestId),
              "warmup", startMs, Long.MAX_VALUE, Long.MAX_VALUE, _brokerId, _brokerId, ""), _threadAccountant)) {
        Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>(finalResponses.size());
        for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : finalResponses.entrySet()) {
          DataTable dataTable = entry.getValue().getDataTable();
          if (dataTable != null) {
            dataTableMap.put(entry.getKey(), dataTable);
          }
        }
        if (!dataTableMap.isEmpty()) {
          _brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, timeoutMs,
              _brokerMetrics);
        }
      } catch (Exception reduceEx) {
        LOGGER.debug("Warmup reduce step failed (continuing); scatter/gather already warmed", reduceEx);
      }
      return System.currentTimeMillis() - startMs;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        // Shutdown interrupted this probe (via shutdownNow); restore the flag so the pool worker unwinds.
        Thread.currentThread().interrupt();
      }
      LOGGER.debug("Warmup probe failed for query on table: {}", tableName, e);
      return -1;
    }
  }

  /// Builds a per-leg probe request carrying the type-suffixed table name that routing resolves against,
  /// from the base (possibly raw-named) request. Deep-copies so the base request is not mutated.
  private static BrokerRequest typedRequest(BrokerRequest base, String tableNameWithType) {
    PinotQuery pinotQuery = base.getPinotQuery().deepCopy();
    pinotQuery.getDataSource().setTableName(tableNameWithType);
    return CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
  }

  /// Whether a probe routes the offline leg: the query targets offline (a type-suffixed offline name) or is
  /// untyped (a raw name, `tableType` null), AND an offline table exists. Static so the offline / realtime /
  /// hybrid decision is unit-testable against a mocked route without a live cluster.
  @VisibleForTesting
  static boolean shouldRouteOffline(@Nullable TableType tableType, TableRouteInfo routeInfo) {
    return tableType != TableType.REALTIME && routeInfo.hasOffline();
  }

  /// Whether a probe routes the realtime leg: the query targets realtime or is untyped, AND a realtime
  /// table exists.
  @VisibleForTesting
  static boolean shouldRouteRealtime(@Nullable TableType tableType, TableRouteInfo routeInfo) {
    return tableType != TableType.OFFLINE && routeInfo.hasRealtime();
  }

  /// Keeps only the configured tables that this broker actually routes. Static for unit testing.
  @VisibleForTesting
  static List<String> filterRoutableTables(RoutingManager routingManager, List<String> tables) {
    Set<String> routable = routingManager.getRoutableTables();
    List<String> result = new ArrayList<>(tables.size());
    for (String table : tables) {
      if (routable.contains(table)) {
        result.add(table);
      } else {
        LOGGER.warn("Broker warmup: configured table {} is not routable on this broker; skipping", table);
      }
    }
    return result;
  }

  /// Picks up to `maxTables` tables that between them cover as many routable servers as possible.
  ///
  /// Greedy set cover rather than "all tables" or a configured list: warmup needs every broker-to-server
  /// channel and the whole query path exercised, and coverage delivers exactly that while degrading
  /// gracefully on a tenant with thousands of tables. Static so it can be unit-tested against a mocked
  /// [RoutingManager] without constructing a broker. When `maxTables` is too small to span every server,
  /// the leftover servers stay unprobed; [#reportServerCoverage] surfaces that as a warning and a metric.
  @VisibleForTesting
  static List<String> selectProbeTables(RoutingManager routingManager, int maxTables) {
    if (maxTables <= 0) {
      return List.of();
    }
    Set<String> uncoveredServers = routableServers(routingManager);
    // Sorted so selection is deterministic across brokers and across restarts.
    List<String> candidates = new ArrayList<>(routingManager.getRoutableTables());
    Collections.sort(candidates);
    List<String> selected = new ArrayList<>(Math.min(maxTables, candidates.size()));
    for (String tableNameWithType : candidates) {
      if (selected.size() >= maxTables || uncoveredServers.isEmpty()) {
        break;
      }
      Set<String> serving = routingManager.getServingInstances(tableNameWithType);
      if (serving == null || serving.isEmpty()) {
        continue;
      }
      if (uncoveredServers.removeAll(serving)) {
        selected.add(tableNameWithType);
      }
    }
    return selected;
  }

  /// Records [BrokerGauge#BROKER_WARMUP_UNCOVERED_SERVERS] and, when non-zero, warns with the routable
  /// servers the chosen probe tables do not route to. For the default set-cover this means `maxTables` was
  /// too small to span the server set (raise it); for a custom `warmup.tables` list it means the list omits
  /// some servers. Warmup still proceeds: those servers' broker-to-server channels are opened
  /// deterministically by startup pre-connect, and the serve-path JIT warmup drives is server-agnostic, so
  /// this is an observability signal, not a failure.
  /// Static (taking its collaborators as parameters) so the gauge emission and warning are unit-testable
  /// against a mocked [RoutingManager] and [BrokerMetrics].
  @VisibleForTesting
  static void reportServerCoverage(RoutingManager routingManager, BrokerMetrics brokerMetrics,
      boolean hasCustomTables, List<String> tables) {
    int totalServers = routableServers(routingManager).size();
    Set<String> uncovered = uncoveredRoutableServers(routingManager, tables);
    brokerMetrics.setValueOfGlobalGauge(BrokerGauge.BROKER_WARMUP_UNCOVERED_SERVERS, uncovered.size());
    if (!uncovered.isEmpty()) {
      LOGGER.warn("Broker warmup probes {} table(s) covering {}/{} routable server(s); {} left unprobed: {}. {}",
          tables.size(), totalServers - uncovered.size(), totalServers, uncovered.size(), uncovered,
          hasCustomTables
              ? "Add tables routing to them to pinot.broker.startup.warmup.tables."
              : "Raise pinot.broker.startup.warmup.maxTables to span every server.");
    }
  }

  /// Returns the routable servers that none of the given probe tables route to. Empty means the tables
  /// span every server this broker can reach. Static so it can be unit-tested against a mocked
  /// [RoutingManager].
  @VisibleForTesting
  static Set<String> uncoveredRoutableServers(RoutingManager routingManager, Collection<String> tables) {
    Set<String> uncovered = routableServers(routingManager);
    for (String tableNameWithType : tables) {
      Set<String> serving = routingManager.getServingInstances(tableNameWithType);
      if (serving != null) {
        uncovered.removeAll(serving);
      }
    }
    return uncovered;
  }

  /// The servers this broker actually routes to: the union of the serving instances of its routable tables.
  ///
  /// Deliberately NOT `getRoutableServerInstanceMap()`, which is every enabled server in the whole cluster
  /// (no tenant filter) -- on a multi-tenant cluster that would count other tenants' servers this broker
  /// never queries, so the coverage metric would be permanently non-zero and the "raise maxTables" warning
  /// would be unactionable. This mirrors how startup pre-connect derives its channels from routing.
  @VisibleForTesting
  static Set<String> routableServers(RoutingManager routingManager) {
    Set<String> servers = new HashSet<>();
    for (String tableNameWithType : routingManager.getRoutableTables()) {
      Set<String> serving = routingManager.getServingInstances(tableNameWithType);
      if (serving != null) {
        servers.addAll(serving);
      }
    }
    return servers;
  }

  /// Returns the next `concurrency` items to probe, round-robin starting at `startSeq`. Kept generic and
  /// static so the rotation -- which must advance across rounds, not reset each round, or only the first
  /// `concurrency` items would ever be probed at low concurrency -- is unit-testable without a live probe.
  /// [Math#floorMod(int,int)] keeps the index valid even if `startSeq` overflows to a negative value.
  @VisibleForTesting
  static <T> List<T> roundRobinBatch(List<T> items, int startSeq, int concurrency) {
    int size = items.size();
    List<T> batch = new ArrayList<>(concurrency);
    for (int i = 0; i < concurrency; i++) {
      batch.add(items.get(Math.floorMod(startSeq + i, size)));
    }
    return batch;
  }

  /// Opens the broker-to-server channels this broker actually routes to, taking the blocking connect --
  /// and, when broker-to-server TLS is on, the handshake -- off the first real query's critical path. The
  /// channels opened are derived from routing (see [#routableChannelTargets]), so an offline-only cluster
  /// opens no REALTIME channels and a broker serving one tenant does not connect to another tenant's
  /// servers. The caller guarantees Helix has converged, so routing reflects the tables and servers this
  /// broker serves. Never throws; returns the number of channels connected before `deadlineMs`.
  @Override
  public int preConnectServers(long deadlineMs) {
    return new ServerPreConnector(() -> routableChannelTargets(_routingManager),
        _queryRouter::preConnect).preConnect(deadlineMs);
  }

  /// Derives the (server, table type) channels this broker actually routes to, so pre-connect opens
  /// exactly those. Iterates the broker's routable tables (each name carries its type) and resolves each
  /// table's serving instances to `ServerInstance`s, deduping across tables. This deliberately does **not**
  /// use `getRoutableServerInstanceMap()` as the server set: that is every enabled server in the whole
  /// cluster, not this broker's, and crossing it with both table types would open a duplicate socket per
  /// server (OFFLINE and REALTIME are separate channels) plus channels to servers this broker never
  /// queries. Static and package-private for unit testing against a mocked [RoutingManager].
  @VisibleForTesting
  static Collection<ServerPreConnector.ChannelTarget> routableChannelTargets(RoutingManager routingManager) {
    Map<String, ServerInstance> serverInstanceMap = routingManager.getRoutableServerInstanceMap();
    Set<ServerPreConnector.ChannelTarget> targets = new HashSet<>();
    for (String tableNameWithType : routingManager.getRoutableTables()) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType == null) {
        continue;
      }
      Set<String> servingInstances = routingManager.getServingInstances(tableNameWithType);
      if (servingInstances == null) {
        continue;
      }
      for (String instanceId : servingInstances) {
        ServerInstance serverInstance = serverInstanceMap.get(instanceId);
        if (serverInstance != null) {
          targets.add(new ServerPreConnector.ChannelTarget(serverInstance, tableType));
        }
      }
    }
    return targets;
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    assert route.getOfflineBrokerRequest() != null || route.getRealtimeBrokerRequest() != null;
    if (requestContext.isSampledRequest()) {
      serverBrokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.TRACE, "true");
    }
    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    ScatterResult scatterResult = doScatter(requestId, rawTableName, route, timeoutMs, serverStats);
    return doReduce(originalBrokerRequest, serverBrokerRequest, scatterResult, scatterGatherStartTimeNs, timeoutMs,
        rawTableName);
  }

  /// Executes scatter-gather: sends the query to servers and collects per-server DataTables.
  /// Subclasses may override to replace or augment the scatter step.
  protected ScatterResult doScatter(long requestId, String rawTableName, TableRouteInfo route, long timeoutMs,
      ServerStats serverStats)
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = _queryRouter.submitQuery(requestId, rawTableName, route, timeoutMs);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    boolean timedOut = asyncQueryResponse.getStatus() == QueryResponse.Status.TIMED_OUT;
    ServerRoutingInstance failedServer = asyncQueryResponse.getFailedServer();
    if (failedServer != null) {
      _failureDetector.markServerUnhealthy(failedServer.getInstanceId(), failedServer.getHostname());
    }
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getServerStats());

    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = Maps.newHashMapWithExpectedSize(finalResponses.size());
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : finalResponses.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
      } else {
        serversNotResponded.add(entry.getKey());
      }
    }
    ScatterResultStats stats = new ScatterResultStats(
        dataTableMap.size() + serversNotResponded.size(), dataTableMap.size(), totalResponseSize);
    return new ScatterResult(dataTableMap, serversNotResponded, stats, timedOut, asyncQueryResponse.getException());
  }

  /// Executes the reduce step on the scatter result and populates the response with server stats.
  /// Subclasses may override to perform custom reduce logic, or construct a [ScatterResult]
  /// with a substituted data table map using [ScatterResultStats] to preserve server stats.
  protected BrokerResponseNative doReduce(BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
      ScatterResult scatterResult, long scatterGatherStartTimeNs, long timeoutMs, String rawTableName)
      throws Exception {
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);

    if (scatterResult.isTimedOut()) {
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS : BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeoutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, serverBrokerRequest,
            scatterResult.getDataTableMap(), reduceTimeoutMs, _brokerMetrics);
    long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(scatterResult.getNumServersQueried());
    brokerResponse.setNumServersResponded(scatterResult.getNumServersResponded());
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(reduceTimeNanos));

    if (scatterResult.getSendException() != null) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND,
          scatterResult.getSendException().getMessage()));
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_SEND_EXCEPTIONS, 1);
    }
    List<ServerRoutingInstance> serversNotResponded = scatterResult.getServersNotResponded();
    if (!serversNotResponded.isEmpty()) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.SERVER_NOT_RESPONDING,
          String.format("%d servers %s not responded", serversNotResponded.size(), serversNotResponded)));
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED
          : BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
        scatterResult.getTotalResponseSize());

    return brokerResponse;
  }

  @Override
  protected BrokerResponseNative processMaterializedViewSplitBrokerRequest(long requestId,
      long materializedViewRequestId, BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute,
      TableRouteInfo materializedViewRoute, long timeoutMs, ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    String rawTableName =
        TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());

    /// Capture a single wall-clock deadline up front and derive every downstream timeout from it.
    /// The split path submits two scatter-gathers AND a reduce; passing `timeoutMs` to each of
    /// them would let two sub-queries individually consume the full budget, leaving the reduce
    /// with a negative remaining and producing silently-truncated results.
    long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    long scatterGatherStartTimeNs = System.nanoTime();

    /// Submit base-table and materialized-view queries in parallel through the `QueryRouter`.
    /// Each route may fan out to multiple servers (especially if the base table is hybrid).
    /// The MV sub-query uses its own request id so it cannot collide with the base sub-query on
    /// servers that receive both requests.
    long submitTimeoutMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime()));
    AsyncQueryResponse baseAsyncResponse =
        _queryRouter.submitQuery(requestId, rawTableName, baseRoute, submitTimeoutMs);
    AsyncQueryResponse materializedViewAsyncResponse =
        _queryRouter.submitQuery(materializedViewRequestId, rawTableName, materializedViewRoute, submitTimeoutMs);

    /// Collect responses from both queries.
    Map<ServerRoutingInstance, ServerResponse> baseFinalResponses = baseAsyncResponse.getFinalResponses();
    Map<ServerRoutingInstance, ServerResponse> viewFinalResponses = materializedViewAsyncResponse.getFinalResponses();

    if (baseAsyncResponse.getStatus() == QueryResponse.Status.TIMED_OUT
        || materializedViewAsyncResponse.getStatus() == QueryResponse.Status.TIMED_OUT) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
    }

    /// Mark failed servers as unhealthy.
    ServerRoutingInstance baseFailedServer = baseAsyncResponse.getFailedServer();
    if (baseFailedServer != null) {
      _failureDetector.markServerUnhealthy(baseFailedServer.getInstanceId(), baseFailedServer.getHostname());
    }
    ServerRoutingInstance viewFailedServer = materializedViewAsyncResponse.getFailedServer();
    if (viewFailedServer != null) {
      _failureDetector.markServerUnhealthy(viewFailedServer.getInstanceId(), viewFailedServer.getHostname());
    }

    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);

    /// Merge `DataTable`s from both base and MV responses into a single map using identity
    /// equality so that `ServerRoutingInstance` objects from different sub-queries never collide.
    /// `ServerRoutingInstance.equals()` keyed on (hostname, port, tableType) can produce the
    /// same hash for base and MV rows on a shared server, causing silent overwrites with a
    /// regular `HashMap`.
    int totalServersQueried = baseFinalResponses.size() + viewFinalResponses.size();
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    long[] totalResponseSizeHolder = {0L};
    Map<ServerRoutingInstance, DataTable> dataTableMap =
        mergeDataTablesByIdentity(baseFinalResponses, viewFinalResponses, serversNotResponded,
            totalResponseSizeHolder);
    long totalResponseSize = totalResponseSizeHolder[0];
    int numServersResponded = dataTableMap.size();

    /// On a SPLIT query, base and MV scatter-gathers cover DISJOINT halves of the timeline
    /// (base covers `ts < boundary`, MV covers `ts >= boundary`).  Partial failure on either
    /// side therefore leaves whole time-ranges missing — semantically distinct from a non-split
    /// hybrid query where partial failure scatters random rows.  Refuse to return a
    /// partially-missing-by-time-range result: throw here so the outer try/catch in
    /// `BaseSingleStageBrokerRequestHandler` bumps `QUERY_REWRITE_EXCEPTIONS` and falls back to
    /// the unsplit base-table query path, which covers the full timeline (and will report any
    /// server failure through the standard non-split error-reporting path).
    int viewSuccessful = countSuccessfulDataTables(viewFinalResponses);
    if (!viewFinalResponses.isEmpty() && viewSuccessful < viewFinalResponses.size()) {
      throw new QueryException(QueryErrorCode.SERVER_NOT_RESPONDING,
          "Materialized view split: " + (viewFinalResponses.size() - viewSuccessful) + " of "
              + viewFinalResponses.size() + " MV server(s) failed to return a DataTable; refusing to "
              + "return a result with missing time ranges");
    }
    int baseSuccessful = countSuccessfulDataTables(baseFinalResponses);
    if (!baseFinalResponses.isEmpty() && baseSuccessful < baseFinalResponses.size()) {
      throw new QueryException(QueryErrorCode.SERVER_NOT_RESPONDING,
          "Materialized view split: " + (baseFinalResponses.size() - baseSuccessful) + " of "
              + baseFinalResponses.size() + " base-table server(s) failed to return a DataTable; refusing "
              + "to return a result with missing time ranges");
    }

    /// Reduce using the original user query so that the correct reducer (selection,
    /// aggregation, group-by) is selected and intermediate results are merged properly.
    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeoutMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - reduceStartTimeNs);
    if (reduceTimeoutMs <= 0) {
      throw new QueryException(QueryErrorCode.BROKER_TIMEOUT,
          "Broker timeout exceeded after MV split scatter-gather; no time remaining for reduce");
    }
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, originalBrokerRequest, dataTableMap,
            reduceTimeoutMs, _brokerMetrics);
    long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(totalServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(reduceTimeNanos));

    /// Propagate send exceptions from both queries.
    Exception baseSendException = baseAsyncResponse.getException();
    if (baseSendException != null) {
      brokerResponse.addException(
          new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND, baseSendException.getMessage()));
    }
    Exception materializedViewSendException = materializedViewAsyncResponse.getException();
    if (materializedViewSendException != null) {
      brokerResponse.addException(
          new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND, materializedViewSendException.getMessage()));
    }
    if (baseSendException != null || materializedViewSendException != null) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_SEND_EXCEPTIONS, 1);
    }

    int numServersNotResponded = serversNotResponded.size();
    if (numServersNotResponded != 0) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.SERVER_NOT_RESPONDING,
          String.format("%d servers %s not responded", numServersNotResponded, serversNotResponded)));
      _brokerMetrics.addMeteredTableValue(rawTableName,
          BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }

  /// Snapshot of server-side scatter statistics. Passed to [ScatterResult] so that server
  /// counts are always derived from the live scatter, not from a data table map that may have been
  /// augmented by a subclass.
  public static final class ScatterResultStats {
    private final int _numServersQueried;
    private final int _numServersResponded;
    private final long _totalResponseSize;

    public ScatterResultStats(int numServersQueried, int numServersResponded, long totalResponseSize) {
      _numServersQueried = numServersQueried;
      _numServersResponded = numServersResponded;
      _totalResponseSize = totalResponseSize;
    }

    public int getNumServersQueried() {
      return _numServersQueried;
    }

    public int getNumServersResponded() {
      return _numServersResponded;
    }

    public long getTotalResponseSize() {
      return _totalResponseSize;
    }
  }

  /// Carries the scatter-gather result before the reduce step.
  public static final class ScatterResult {
    private final Map<ServerRoutingInstance, DataTable> _dataTableMap;
    private final List<ServerRoutingInstance> _serversNotResponded;
    private final long _totalResponseSize;
    private final boolean _timedOut;
    private final Exception _sendException;
    private final int _numServersQueried;
    private final int _numServersResponded;

    public ScatterResult(Map<ServerRoutingInstance, DataTable> dataTableMap,
        List<ServerRoutingInstance> serversNotResponded, ScatterResultStats stats,
        boolean timedOut, Exception sendException) {
      _dataTableMap = dataTableMap;
      _serversNotResponded = serversNotResponded;
      _totalResponseSize = stats.getTotalResponseSize();
      _timedOut = timedOut;
      _sendException = sendException;
      _numServersQueried = stats.getNumServersQueried();
      _numServersResponded = stats.getNumServersResponded();
    }

    public Map<ServerRoutingInstance, DataTable> getDataTableMap() {
      return _dataTableMap;
    }

    public List<ServerRoutingInstance> getServersNotResponded() {
      return _serversNotResponded;
    }

    public int getNumServersQueried() {
      return _numServersQueried;
    }

    public int getNumServersResponded() {
      return _numServersResponded;
    }

    public long getTotalResponseSize() {
      return _totalResponseSize;
    }

    public boolean isTimedOut() {
      return _timedOut;
    }

    public Exception getSendException() {
      return _sendException;
    }
  }

  /// Check if a server that was previously detected as unhealthy is now healthy.
  public FailureDetector.ServerState retryUnhealthyServer(String instanceId) {
    LOGGER.info("Retrying unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);

    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    // Could occur if the cluster is only serving multi-stage queries
    if (!_queryRouter.hasChannel(serverInstance)) {
      return FailureDetector.ServerState.UNKNOWN;
    }

    if (_queryRouter.connect(serverInstance)) {
      LOGGER.info("Successfully connect to server: {}, marking it healthy", instanceId);
      return FailureDetector.ServerState.HEALTHY;
    } else {
      LOGGER.warn("Still cannot connect to server: {}, retry later", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }
  }

  /// Counts responses that successfully returned a DataTable. Production callers use this in the
  /// MV-split path to detect "all of one side's servers failed" — if `viewFinalResponses` is
  /// non-empty AND the count is zero, the split would silently undercount the historical half
  /// (and symmetrically for the base side), so the caller throws to trigger the outer fallback.
  /// Package-private for direct unit-test coverage of the guard's boolean.
  @VisibleForTesting
  static int countSuccessfulDataTables(Map<ServerRoutingInstance, ServerResponse> responses) {
    int count = 0;
    for (ServerResponse r : responses.values()) {
      if (r.getDataTable() != null) {
        count++;
      }
    }
    return count;
  }

  /// Merges base and MV server responses into one IdentityHashMap whose entries are accumulated
  /// by reference equality rather than by `ServerRoutingInstance.equals()`. The MV split path
  /// can route both sub-queries to the same physical server (same hostname+port+tableType, so
  /// `equals()` matches) but with distinct `ServerRoutingInstance` instances — a regular HashMap
  /// would silently overwrite one DataTable with the other and produce under-counted results.
  ///
  /// Package-private so the test suite can pin this contract without spinning up a broker.
  @VisibleForTesting
  static Map<ServerRoutingInstance, DataTable> mergeDataTablesByIdentity(
      Map<ServerRoutingInstance, ServerResponse> baseResponses,
      Map<ServerRoutingInstance, ServerResponse> viewResponses,
      List<ServerRoutingInstance> serversNotResponded, long[] totalResponseSizeHolder) {
    int totalServers = baseResponses.size() + viewResponses.size();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new IdentityHashMap<>(totalServers);
    long totalResponseSize = 0;
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : baseResponses.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
      } else {
        serversNotResponded.add(entry.getKey());
      }
    }
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : viewResponses.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
      } else {
        serversNotResponded.add(entry.getKey());
      }
    }
    totalResponseSizeHolder[0] = totalResponseSize;
    return dataTableMap;
  }
}
