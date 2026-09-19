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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
  /// Upper bound on the local (stage 1) warmup iterations. Enough to drive the trivial compile + serialize
  /// path to JIT, but decoupled from `minIterations` (which governs the far more expensive NETWORK probe
  /// depth): a large `minIterations` must not turn stage 1 into a local spin that eats the whole budget
  /// before the network probe runs.
  private static final int LOCAL_WARMUP_MAX_ITERATIONS = 2_000;
  /// How long `warmUp` waits after `shutdownNow` for interrupted probe workers to unwind before returning,
  /// so a probe cannot outlive the call and race the request handler being torn down on shutdown. Bounded
  /// so shutdown never hangs on it; interrupted probes unwind well within this.
  private static final long PROBE_POOL_SHUTDOWN_WAIT_MS = 1_000L;
  /// Backoff after an unproductive probe round (nothing routable / every probe failed), so a not-yet-ready
  /// cluster cannot busy-spin the warmup thread for the whole budget during startup.
  private static final long WARMUP_FAILURE_BACKOFF_MS = 100L;
  /// Warmup reduces are best-effort JIT warming whose result is discarded, so their metrics go to the shared
  /// no-op instance instead of the broker's real counters -- otherwise synthetic startup probes would
  /// pollute latency timers, documentsScanned, and the per-table meters before any real traffic. Safe to
  /// share across the concurrent probe threads: every increment on a no-op registry is a no-op.
  private static final BrokerMetrics WARMUP_NOOP_METRICS = BrokerMetrics.noop();

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

  /// Warms this broker's serve path before readiness is granted, in two stages:
  ///   1. a local, no-network pass ([#warmUpLocal]) that JIT-warms the compile and response-serialization
  ///      paths, so they are warm even if no server is reachable; then
  ///   2. a network probe -- repeatedly running real probe queries until they have run enough times (the
  ///      `minIterations` depth floor) or the budget expires -- which warms the scatter/gather/deserialize
  ///      and reduce paths and opens the broker-to-server channels lazily as it goes.
  ///
  /// Probes go through [QueryRouter] rather than [#handleRequest], deliberately bypassing access control,
  /// query quota and the query log -- warmup then needs no synthetic identity and pollutes no
  /// customer-facing surface (probe reduces also use a throwaway metrics registry, see
  /// [#WARMUP_NOOP_METRICS]).
  ///
  /// Contract for the readiness caller: this **never throws** and **always returns by `deadlineMs`** (an
  /// absolute [System#currentTimeMillis] value). Every wait -- each probe and each `Future#get` -- is
  /// bounded by the remaining budget, and once it is spent all outstanding probes are cancelled, so a slow
  /// or unreachable server can never stall the rolling restart this gates. The return value (reached the
  /// floor vs. hit the budget) is for logging and metrics only; the caller proceeds either way.
  ///
  /// Runs on the single `broker-startup-warmup` thread. The probe pool it creates is local to the call and
  /// shut down before returning, so there is no cross-call shared mutable state.
  @Override
  public boolean warmUp(BrokerWarmupConfig config, long deadlineMs) {
    // Stage 1: local, no-network warmup of the compile + response-serialization paths, run before the
    // network probe so they are warm even if no server is reachable. Looped enough to JIT the serialization
    // path (a single pass leaves it interpreted): minIterations iterations, capped at 2000
    // (LOCAL_WARMUP_MAX_ITERATIONS -- so the cap only bites if minIterations is raised above 2000) and
    // deadline-guarded, so it stays a quick prelude and never eats the budget the network probe needs.
    warmUpLocal(Math.min(config.minIterations(), LOCAL_WARMUP_MAX_ITERATIONS), deadlineMs);
    int concurrency = Math.max(1, config.concurrency());
    ExecutorService probePool = Executors.newFixedThreadPool(concurrency,
        new ThreadFactoryBuilder().setNameFormat("broker-warmup-probe-%d").setDaemon(true).build());
    // Tables at least one probe actually reached the servers for (probe() adds to it from the pool threads,
    // so it must be thread-safe). Read below to report per-server coverage -- only AFTER the pool drains, so
    // no in-flight probe is still writing it.
    Set<String> probedTables = ConcurrentHashMap.newKeySet();
    try {
      return warmUpNetwork(config, deadlineMs, probePool, concurrency, probedTables);
    } catch (Exception e) {
      LOGGER.warn("Broker warmup failed; proceeding without it", e);
      return false;
    } finally {
      probePool.shutdownNow();
      awaitPoolDrain(probePool, PROBE_POOL_SHUTDOWN_WAIT_MS);
      // Pool drained: probedTables is now stable. Report which routable servers this run warmed (0 on a
      // clean floor exit; non-zero if the budget expired before the round-robin reached every server).
      // Guarded so this finally can never make warmUp throw (the interface contract is "never throws").
      try {
        reportServerCoverage(_routingManager, _brokerMetrics, probedTables);
      } catch (Exception e) {
        LOGGER.debug("Warmup coverage report failed (continuing)", e);
      }
    }
  }

  /// Waits up to `waitMs` for `pool` to terminate after a `shutdownNow`, so an interrupted probe worker
  /// unwinds before `warmUp` returns and cannot race the request handler being torn down on shutdown.
  ///
  /// Crucially this **saves and clears** the interrupt flag around the wait. On the shutdown path the warmup
  /// thread is interrupted (`stopWarmup` -> `interrupt`), and `awaitTermination` is interruptible -- with the
  /// flag set it throws `InterruptedException` on entry and waits 0ms, skipping the drain on exactly the path
  /// it exists for. Clearing the flag lets the bounded wait actually happen; the flag is restored afterwards.
  /// Static and package-private so the interrupted-path behavior is unit-testable.
  @VisibleForTesting
  static void awaitPoolDrain(ExecutorService pool, long waitMs) {
    boolean interrupted = Thread.interrupted();
    try {
      if (!pool.awaitTermination(waitMs, TimeUnit.MILLISECONDS)) {
        LOGGER.debug("Warmup probe pool did not fully terminate within {} ms; proceeding", waitMs);
      }
    } catch (InterruptedException e) {
      interrupted = true;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /// Stage 1: local, no-network warmup. The network probe goes through [QueryRouter] and stops at the
  /// gathered DataTables, so it never exercises the response build + JSON serialization the first real query
  /// pays (the compile path it does re-warm, but only via a real table). This compiles a throwaway query and
  /// serializes a small synthetic [BrokerResponseNative] `iterations` times so the serialization path
  /// reaches JIT rather than staying interpreted after a single pass. `iterations` is capped by the caller
  /// (see [#LOCAL_WARMUP_MAX_ITERATIONS]); the loop is guarded by both `deadlineMs` and the interrupt flag,
  /// so stage 1 stays a quick prelude and stops promptly when shutdown interrupts the warmup thread. Never
  /// throws; never runs the network.
  private void warmUpLocal(int iterations, long deadlineMs) {
    try {
      for (int i = 0; i < iterations && System.currentTimeMillis() < deadlineMs
          && !Thread.currentThread().isInterrupted(); i++) {
        CalciteSqlCompiler.compileToBrokerRequest("SELECT 1");
        BrokerResponseNative response = new BrokerResponseNative();
        response.setResultTable(new ResultTable(
            new DataSchema(new String[]{"warmup"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG}),
            Collections.singletonList(new Object[]{1L})));
        response.setNumDocsScanned(1);
        response.toJsonString();
      }
    } catch (Exception e) {
      LOGGER.debug("Local (stage 1) warmup failed; continuing", e);
    }
  }

  /// Fires the given probe tasks concurrently on the shared pool and returns the latencies (ms) of the
  /// probes that completed successfully. Empty means the whole round was unproductive (nothing routable
  /// yet / all failed); a failed probe simply does not contribute.
  ///
  /// The total wait is bounded by `deadlineMs`, never by a fixed per-probe constant: each `get()` waits
  /// only the remaining budget, and once the deadline has passed every still-outstanding future is
  /// cancelled instead of waited on. This is what makes the budget a hard ceiling even when more tasks were
  /// submitted than the pool has threads (so tasks queue) -- otherwise a queued straggler could hold the
  /// readiness gate well past the budget.
  ///
  /// Static and package-private so the budget / cancellation / interrupt semantics can be unit-tested with
  /// injected probe callables, without a live cluster. `firstProbeThrowLogged` is a 1-element per-run latch
  /// so a probe that *throws* is surfaced once for the whole warmup run, not once per round.
  @VisibleForTesting
  static List<Long> runConcurrentRound(List<Callable<Long>> tasks, ExecutorService pool, long deadlineMs,
      boolean[] firstProbeThrowLogged) {
    List<Future<Long>> futures = new ArrayList<>(tasks.size());
    for (Callable<Long> task : tasks) {
      futures.add(pool.submit(task));
    }
    List<Long> latencies = new ArrayList<>(futures.size());
    for (int i = 0; i < futures.size(); i++) {
      long remainingMs = deadlineMs - System.currentTimeMillis();
      if (remainingMs <= 0) {
        // Budget spent: stop waiting and cancel everything still outstanding so no probe outlives it.
        cancelAll(futures, i);
        break;
      }
      Future<Long> future = futures.get(i);
      try {
        Long elapsedMs = future.get(remainingMs, TimeUnit.MILLISECONDS);
        if (elapsedMs != null && elapsedMs >= 0) {
          latencies.add(elapsedMs);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelAll(futures, i);
        return latencies;
      } catch (ExecutionException e) {
        // The probe task itself threw (not a timeout). It does not count; surface the first one of the whole
        // run so a consistently broken probe is diagnosable without a debug rebuild.
        if (!firstProbeThrowLogged[0]) {
          firstProbeThrowLogged[0] = true;
          LOGGER.warn("Broker warmup probe threw (further occurrences suppressed)", e.getCause());
        }
        future.cancel(true);
      } catch (Exception e) {
        // TimeoutException (probe overran the remaining budget) or CancellationException: expected, does not
        // count, and is cancelled so it does not keep running behind the next round (no-op if already done).
        future.cancel(true);
      }
    }
    return latencies;
  }

  private static void cancelAll(List<Future<Long>> futures, int from) {
    for (int j = from; j < futures.size(); j++) {
      futures.get(j).cancel(true);
    }
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

  /// The network warmup: probe the static `SELECT * FROM "<t>" LIMIT 1` over a greedy set-cover of tables
  /// covering every routable server. Exits once at least `minIterations` probes have completed successfully
  /// -- a depth floor that guarantees enough invocations to drive the query path's JIT to its top tier --
  /// OR the budget expires. A latency target is deliberately not used: a trivial probe reaches low latency
  /// after a handful of iterations while the code is still only partially compiled, so latency is a false
  /// early-exit signal.
  private boolean warmUpNetwork(BrokerWarmupConfig config, long deadlineMs, ExecutorService pool,
      int concurrency, Set<String> probedTables) {
    long successfulProbes = 0;
    int rounds = 0;
    // Select the probe tables once and reuse them across rounds. Only re-select while the result is empty
    // (routing not populated yet); recomputing the greedy set-cover every round would repeat O(tables) work
    // up to minIterations times on a large-table tenant, for a result that does not change once non-empty.
    List<String> tables = List.of();
    // Monotonic across rounds so the round-robin actually advances through every covered table even at
    // concurrency 1; resetting per round would probe only the first `concurrency` tables forever.
    int probeSeq = 0;
    boolean reachedFloor = false;
    // Per-run latch so a probe that throws is logged once for the whole run, not once per round.
    boolean[] firstProbeThrowLogged = new boolean[1];
    while (System.currentTimeMillis() < deadlineMs && !Thread.currentThread().isInterrupted()) {
      if (tables.isEmpty()) {
        tables = selectProbeTables(_routingManager);
      }
      if (tables.isEmpty()) {
        // Converged but nothing routable yet: back off and retry rather than declaring the broker warm --
        // an empty routing table here would otherwise make the gate a no-op precisely on a cold broker.
        if (!warmupBackoff()) {
          return false;
        }
        continue;
      }
      if (System.currentTimeMillis() >= deadlineMs) {
        break;
      }
      rounds++;
      // A batch of `concurrency` probes, round-robin over the covered tables (advancing across rounds via
      // probeSeq): exercises every server (coverage) AND real concurrency (contention) at once. Each task
      // derives its own timeout from the deadline when it actually starts (see probeWithinDeadline), so a
      // task that queued behind the pool cannot overrun the budget.
      List<Callable<Long>> tasks = new ArrayList<>(concurrency);
      for (String tableNameWithType : roundRobinBatch(tables, probeSeq, concurrency)) {
        tasks.add(() -> probeWithinDeadline(compileProbe(tableNameWithType), deadlineMs, probedTables));
      }
      probeSeq += concurrency;
      List<Long> latencies = runConcurrentRound(tasks, pool, deadlineMs, firstProbeThrowLogged);
      if (latencies.isEmpty()) {
        if (!warmupBackoff()) {
          return false;
        }
        continue;
      }
      successfulProbes += latencies.size();
      if (successfulProbes >= config.minIterations()) {
        reachedFloor = true;
        break;
      }
    }
    // Coverage is reported by the caller after the pool drains (probedTables must be stable). Here we only
    // signal floor vs. budget.
    if (reachedFloor) {
      LOGGER.info("Broker warmup completed after {} round(s) at concurrency {}; {} probes (floor {})", rounds,
          concurrency, successfulProbes, config.minIterations());
      return true;
    }
    logBudgetExpiry(rounds, concurrency, successfulProbes, config.minIterations());
    return false;
  }

  /// Logs warmup budget expiry: WARN only when nothing warmed (`successfulProbes == 0`), INFO otherwise.
  /// Expiring after warming some probes is the expected steady-state exit for a probe too expensive to reach
  /// the floor within the budget -- it warmed as much as the budget allowed -- so it is not warning-worthy.
  private static void logBudgetExpiry(int rounds, int concurrency, long successfulProbes, int floor) {
    if (successfulProbes == 0) {
      LOGGER.warn("Broker warmup budget expired after {} round(s) at concurrency {}; {} probes (floor {}). "
          + "Proceeding to serve traffic (nothing warmed).", rounds, concurrency, successfulProbes, floor);
    } else {
      LOGGER.info("Broker warmup budget expired after {} round(s) at concurrency {}; {} probes (floor {}). "
          + "Proceeding to serve traffic.", rounds, concurrency, successfulProbes, floor);
    }
  }

  /// Compiles the default probe query for a single physical table.
  private BrokerRequest compileProbe(String tableNameWithType) {
    return CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\" LIMIT 1");
  }

  /// Runs one probe with a timeout derived from the remaining budget **at the moment the task starts**,
  /// never a fixed per-probe constant. When more tasks are submitted than the pool has threads they queue,
  /// and a queued task may not start until much of the budget is already gone; deriving the timeout here
  /// (rather than when the round was built) is what keeps the budget a hard ceiling. Returns -1 (uncounted)
  /// if the budget is already spent when the task starts, so no probe is fired past the deadline.
  private long probeWithinDeadline(BrokerRequest brokerRequest, long deadlineMs, Set<String> probedTables) {
    long timeoutMs = Math.min(deadlineMs - System.currentTimeMillis(), WARMUP_PROBE_TIMEOUT_MS);
    return timeoutMs <= 0 ? -1 : probe(brokerRequest, timeoutMs, probedTables);
  }

  /// Issues one probe query through [QueryRouter] and returns its wall-clock duration in ms, or -1 on
  /// failure. Builds the route the same way the normal path does, minus auth/quota/logging. When at least
  /// one server responds, records the (type-suffixed) table name in `probedTables` so the caller can tell,
  /// at exit, which routable servers were actually warmed.
  private long probe(BrokerRequest brokerRequest, long timeoutMs, Set<String> probedTables) {
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
      // The probe table is always type-suffixed (the set-cover picks names straight from getRoutableTables),
      // so it targets exactly one type. Each leg's request must carry the type-suffixed table name -- that
      // is what routing resolves against -- so build a per-leg request from the base query (as the normal
      // path does). shouldRouteOffline/Realtime still handle the general case for robustness.
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
      // calculateRoutes sets the per-leg broker requests on the routeInfo itself (nulling a leg whose
      // routing table turns out empty), so we pass them as arguments and do not pre-set them here.
      _implicitHybridTableRouteProvider.calculateRoutes(routeInfo, _routingManager, offlineBrokerRequest,
          realtimeBrokerRequest, requestId);
      if (!routeInfo.isRouteExists()) {
        return -1;
      }
      long startMs = System.currentTimeMillis();
      // Scatter/gather (+ DataTable deserialize on receive) is the dominant cost and does not need a
      // QueryThreadContext; run it directly so its warmth always counts toward the probe latency.
      // Side effect worth naming: submitQuery records adaptive-server-selector stats (AsyncQueryResponse ->
      // ServerRoutingStatsManager), so probes seed each server's latency EMA before real traffic. A probe
      // that gets a real response seeds the EMA with that server's (cold) response latency; a probe that
      // times out or errors seeds it with the full timeout (see AsyncQueryResponse#getFinalResponses). On a
      // healthy cluster every probe responds, so the seeds are comparable across servers and the EMA decays
      // to true warm latencies within a few real requests -- a small, self-correcting bias. The one case
      // that biases RELATIVE ordering is a server slow enough to time out probes while its peers respond:
      // it is seeded high and the selector routes less to it at first. That is acceptable (it steers early
      // traffic away from a genuinely slow server and self-corrects), and still strictly better than the
      // selector starting with no per-server history at all.
      AsyncQueryResponse response = _queryRouter.submitQuery(requestId, rawTableName, routeInfo, timeoutMs);
      Map<ServerRoutingInstance, ServerResponse> finalResponses = response.getFinalResponses();
      // Coverage: if at least one server returned data, this table's servers were reached this run. A table
      // whose servers all timed out is deliberately NOT recorded, so it counts as uncovered at exit.
      for (ServerResponse serverResponse : finalResponses.values()) {
        if (serverResponse.getDataTable() != null) {
          probedTables.add(tableName);
          break;
        }
      }
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
          // Discard-only warmth: route metrics to the throwaway registry so the probe does not pollute the
          // broker's real counters (see WARMUP_NOOP_METRICS).
          _brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, timeoutMs,
              WARMUP_NOOP_METRICS);
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

  /// Picks the fewest tables that between them cover **every** routable server -- the one, only behavior:
  /// warmup always covers the whole fleet, with no cap to tune.
  ///
  /// Greedy set cover rather than "all tables" or a configured list: warmup needs every broker-to-server
  /// channel and the whole query path exercised, and coverage delivers exactly that. The loop stops as soon
  /// as every server is covered, so it self-limits to at most one table per server (never the full table
  /// list) even on a tenant with thousands of tables. Because the coverage universe is exactly the servers
  /// some routable table serves, this always achieves full coverage. Static so it can be unit-tested against
  /// a mocked [RoutingManager] without constructing a broker.
  @VisibleForTesting
  static List<String> selectProbeTables(RoutingManager routingManager) {
    Set<String> uncoveredServers = routableServers(routingManager);
    // Sorted so selection is deterministic across brokers and across restarts.
    List<String> candidates = new ArrayList<>(routingManager.getRoutableTables());
    Collections.sort(candidates);
    // Capacity bounded by the server count -- the true ceiling on how many tables get selected.
    List<String> selected = new ArrayList<>(Math.min(uncoveredServers.size(), candidates.size()));
    for (String tableNameWithType : candidates) {
      if (uncoveredServers.isEmpty()) {
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

  /// Records [BrokerGauge#STARTUP_WARMUP_UNCOVERED_SERVERS] on warmup exit: the routable servers that
  /// `probedTables` (the tables at least one probe actually reached this run) do NOT, between them, route
  /// to. The set-cover guarantees the *selected* tables span every server, so this is `0` when warmup reaches
  /// its floor (every table probed); it goes non-zero only when warmup exits early -- the budget expired, or
  /// servers were too slow, before the round-robin reached every table.
  ///
  /// It is a warmup **completeness** signal, not "these servers are stone cold": the broker's serve-path JIT
  /// is warmed per-JVM (not per-server) and channels are opened by pre-connect, so an unreached server is
  /// only marginally colder. Read a non-zero value as "warmup ran out of budget before its intended
  /// coverage", most meaningful alongside whether the floor was reached.
  ///
  /// Uncovered is derived from a single [#routableServers] read (via [#uncoveredRoutableServers]) so the
  /// count is self-consistent. Static (collaborators as parameters) so the emission is unit-testable against
  /// a mocked [RoutingManager] and [BrokerMetrics].
  @VisibleForTesting
  static void reportServerCoverage(RoutingManager routingManager, BrokerMetrics brokerMetrics,
      Collection<String> probedTables) {
    Set<String> uncovered = uncoveredRoutableServers(routingManager, probedTables);
    brokerMetrics.setValueOfGlobalGauge(BrokerGauge.STARTUP_WARMUP_UNCOVERED_SERVERS, uncovered.size());
    if (!uncovered.isEmpty()) {
      LOGGER.warn("Broker warmup did not reach {} routable server(s) before it exited (budget expired or "
          + "servers too slow): {}. Serve-path JIT is warmed regardless; those servers are only marginally "
          + "colder.", uncovered.size(), uncovered);
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
  /// never queries, so the coverage metric would be permanently non-zero and its warning unactionable. This
  /// mirrors how startup pre-connect derives its channels from routing.
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
    if (size == 0) {
      // Defensive: an empty list would make the floorMod below divide by zero. Callers already guard, but a
      // future caller must get an empty batch, not an ArithmeticException.
      return List.of();
    }
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
