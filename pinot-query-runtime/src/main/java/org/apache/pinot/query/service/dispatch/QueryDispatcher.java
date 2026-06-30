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
package org.apache.pinot.query.service.dispatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ConnectivityState;
import io.grpc.Deadline;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.runtime.PairList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainConverterDispatcher;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageStatsTreeNode;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.PlanVersions;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.ServerResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);
  private static final String PINOT_BROKER_QUERY_DISPATCHER_FORMAT = "multistage-query-dispatch-%d";
  /**
   * Maximum time (ms) to wait for outstanding {@code OpChainComplete} stats messages on both the success and error
   * paths of a stream-mode query:
   * <ul>
   *   <li><b>Success path:</b> after the broker receiving mailbox has finished (data is already in hand), the broker
   *       waits up to this long for remaining stats before returning the result to the client.</li>
   *   <li><b>Error path:</b> after a fan-out cancel has been issued, the broker waits up to this long for partial
   *       stats before building the error result.</li>
   * </ul>
   * In both cases stats collection is best-effort — a single slow opchain must not hold the client response until
   * the full query deadline. The wait window is configurable via
   * {@link CommonConstants.Broker#CONFIG_OF_STREAM_STATS_DRAIN_MS}.
   */
  private final long _statsDrainMs;

  private final MailboxService _mailboxService;
  private final ExecutorService _executorService;
  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();
  @Nullable
  private final TlsConfig _tlsConfig;
  @Nullable
  private final SslContext _clientGrpcSslContext;
  private final DispatchClient.KeepAliveConfig _keepAliveConfig;
  // maps broker-generated query id to the set of servers that the query was dispatched to
  private final Map<Long, Set<QueryServerInstance>> _serversByQuery;
  private final FailureDetector _failureDetector;
  private final Duration _cancelTimeout;
  /// Cluster-level default for stream-stats mode. Used as the fallback in {@link #submitAndReduce} when the query
  /// does not carry an explicit {@link QueryOptionKey#STREAM_STATS} override.
  private final boolean _streamStatsDefault;

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation, Duration cancelTimeout) {
    this(mailboxService, failureDetector, tlsConfig, enableCancellation, cancelTimeout,
        DispatchClient.KeepAliveConfig.DISABLED, false, CommonConstants.Broker.DEFAULT_STREAM_STATS_DRAIN_MS);
  }

  /// Overload that accepts gRPC keep-alive settings for broker dispatch channels. A non-positive `keepAliveTimeMs`
  /// disables keep-alive.
  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation, Duration cancelTimeout, int keepAliveTimeMs, int keepAliveTimeoutMs,
      boolean keepAliveWithoutCalls, boolean streamStatsDefault, long statsDrainMs) {
    this(mailboxService, failureDetector, tlsConfig, enableCancellation, cancelTimeout,
        new DispatchClient.KeepAliveConfig(keepAliveTimeMs, keepAliveTimeoutMs, keepAliveWithoutCalls),
        streamStatsDefault, statsDrainMs);
  }

  private QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation, Duration cancelTimeout, DispatchClient.KeepAliveConfig keepAliveConfig,
      boolean streamStatsDefault, long statsDrainMs) {
    _cancelTimeout = cancelTimeout;
    _statsDrainMs = statsDrainMs;
    _mailboxService = mailboxService;
    _executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
        new TracedThreadFactory(Thread.NORM_PRIORITY, false, PINOT_BROKER_QUERY_DISPATCHER_FORMAT));
    _tlsConfig = tlsConfig;
    _clientGrpcSslContext = initClientSslContext(tlsConfig);
    _keepAliveConfig = keepAliveConfig;
    _failureDetector = failureDetector;
    _streamStatsDefault = streamStatsDefault;

    if (enableCancellation) {
      _serversByQuery = new ConcurrentHashMap<>();
    } else {
      _serversByQuery = null;
    }
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public void start() {
    _mailboxService.start();
  }

  /// Submits a query to the server and waits for the result.
  ///
  /// This method may throw almost any exception but QueryException or TimeoutException, which are caught and converted
  /// into a QueryResult with the error code (and stats, if any can be collected).
  public QueryResult submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions)
      throws Exception {
    return submitAndReduce(context, dispatchableSubPlan, timeoutMs, queryOptions, null);
  }

  /// Same as {@link #submitAndReduce(RequestContext, DispatchableSubPlan, long, Map)} but records per-server
  /// in-flight request statistics into {@code statsManager} for use by the adaptive query router.
  /// When {@code statsManager} is non-null:
  /// <ul>
  ///   <li>Each leaf server is registered as having one more in-flight request via
  ///       {@link ServerRoutingStatsManager#recordStatsForQuerySubmission} after the fan-out begins.</li>
  ///   <li>After the full fan-out completes (or fails), each server is decremented via
  ///       {@link ServerRoutingStatsManager#recordStatsUponResponseArrival} with {@code latency = -1}
  ///       (no latency is recorded at this stage).</li>
  /// </ul>
  /// TODO: Replace the coarse end-of-fanout decrement with per-sender arrival once per-sender EOS
  ///       interception is in place, and record real leaf-stage latency at that point.
  public QueryResult submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions, @Nullable ServerRoutingStatsManager statsManager)
      throws Exception {
    if (QueryOptionsUtils.isStreamStats(queryOptions, _streamStatsDefault)) {
      return submitAndReduceWithStream(context, dispatchableSubPlan, timeoutMs, queryOptions, statsManager);
    }
    long requestId = context.getRequestId();
    Set<QueryServerInstance> servers = new HashSet<>();
    // Tracks servers where recordStatsForQuerySubmission was actually called, so the finally block only
    // decrements servers that were incremented — guarding against a partial failure in submit().
    Set<QueryServerInstance> incrementedServers = new HashSet<>();
    try {
      submit(requestId, dispatchableSubPlan, timeoutMs, servers, queryOptions);
      // The SSE engine increments before `submit`, but here we increment after because `submit` populates
      // the list of servers. Getting the list of servers before calling `submit` would expose
      // implementation details of `submit`.
      if (statsManager != null) {
        for (QueryServerInstance server : servers) {
          statsManager.recordStatsForQuerySubmission(requestId, server.getInstanceId());
          incrementedServers.add(server);
        }
      }
      QueryResult result = runReducer(dispatchableSubPlan, queryOptions, _mailboxService);
      if (result.getProcessingException() != null) {
        cancel(requestId);
      }
      return result;
    } catch (Exception ex) {
      return tryRecover(context.getRequestId(), servers, ex);
    } catch (Throwable e) {
      // TODO: Consider always cancel when it returns (early terminate)
      cancel(requestId);
      throw e;
    } finally {
      if (statsManager != null) {
        for (QueryServerInstance server : incrementedServers) {
          statsManager.recordStatsUponResponseArrival(requestId, server.getInstanceId(), -1);
        }
      }
      if (isQueryCancellationEnabled()) {
        _serversByQuery.remove(requestId);
      }
    }
  }

  /// Streaming variant of {@link #submitAndReduce}: opens one {@code SubmitWithStream} bidi RPC per server, runs the
  /// broker's stage 0 reducer, and once the receiving mailbox finishes awaits the per-stage stats with early
  /// completion (returns as soon as every expected opchain has reported, or when the wait window fires — whichever
  /// happens first). Stats from the session accumulator are then merged into the broker's local stage 0 stats to
  /// build the final {@link QueryResult}.
  ///
  /// The wait window is bounded by the query's remaining timeout: if {@code submitWithStream + runReducer} consumed
  /// most of the budget, the per-stage stats may end up partial (visible via the per-stage {@code mergeFailed} /
  /// {@code missing} counts the session exposes).
  ///
  /// Cancel is handled via {@link StreamingQuerySession#fanOutCancel()} — no unary Cancel RPCs are issued for this
  /// query path. On any error, fan-out cancel is broadcast over the open streams, then the broker waits for remaining
  /// stats before building the final result.
  ///
  /// <b>Mixed-version policy.</b> No automatic fallback to the unary {@link #submit} path. Enabling
  /// {@link CommonConstants.Broker.Request.QueryOptionKey#STREAM_STATS} requires every server in the
  /// cluster to implement {@code SubmitWithStream}; if any server returns {@code UNIMPLEMENTED} or any other
  /// transport error during dispatch, {@link #submitWithStream} surfaces the throwable through the ack queue,
  /// {@link #processResults} throws, and this method fans out cancel via the session before propagating the failure.
  private QueryResult submitAndReduceWithStream(RequestContext context, DispatchableSubPlan dispatchableSubPlan,
      long timeoutMs, Map<String, String> queryOptions, @Nullable ServerRoutingStatsManager statsManager)
      throws Exception {
    long requestId = context.getRequestId();
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    Set<QueryServerInstance> servers = new HashSet<>();
    // Tracks servers where recordStatsForQuerySubmission was actually called, so the finally block only decrements
    // servers that were incremented — same contract as the legacy submitAndReduce path, see its Javadoc.
    Set<QueryServerInstance> incrementedServers = new HashSet<>();

    // The session's expected-opchain count must equal the total number of opchains across every (server, non-root
    // stage) pair — that's how many OpChainComplete messages we expect to receive.
    Set<DispatchablePlanFragment> stagePlansWithoutRoot = dispatchableSubPlan.getQueryStagesWithoutRoot();
    int totalExpected = 0;
    Map<Integer, Integer> expectedByStage = new HashMap<>();
    for (DispatchablePlanFragment stagePlan : stagePlansWithoutRoot) {
      int stageId = stagePlan.getPlanFragment().getFragmentId();
      int stageCount = 0;
      for (List<Integer> workerIds : stagePlan.getServerInstanceToWorkerIdMap().values()) {
        stageCount += workerIds.size();
      }
      totalExpected += stageCount;
      expectedByStage.put(stageId, stageCount);
    }
    StreamingQuerySession session = new StreamingQuerySession(requestId, totalExpected);

    try {
      submitWithStream(requestId, dispatchableSubPlan, timeoutMs, servers, queryOptions, session);
      // Increment after submitWithStream populates the server list, mirroring the legacy path (which increments
      // after submit for the same reason).
      if (statsManager != null) {
        for (QueryServerInstance server : servers) {
          statsManager.recordStatsForQuerySubmission(requestId, server.getInstanceId());
          incrementedServers.add(server);
        }
      }
      QueryResult brokerResult = runReducer(dispatchableSubPlan, queryOptions, _mailboxService);

      // If the reducer surfaced an error, cancel the still-running opchains BEFORE waiting for stats: they won't
      // complete (and report) until cancelled, so waiting first would burn the drain window on reports that cannot
      // arrive and delay the cancel by up to that window. Mirrors the tryRecoverWithStream ordering.
      if (brokerResult.getProcessingException() != null) {
        session.fanOutCancel();
      }

      // Receiving mailbox finished — data is ready. Wait for stats on a best-effort basis; cap at
      // _statsDrainMs so a single slow opchain cannot delay the client response.
      long statsWaitMs = Math.min(_statsDrainMs, Math.max(0, deadlineMs - System.currentTimeMillis()));
      boolean fullCoverage = session.awaitCompletion(statsWaitMs, TimeUnit.MILLISECONDS);
      if (!fullCoverage) {
        LOGGER.warn("Stream-mode request {} timed out waiting for stats after mailbox EOS; coverage may be partial",
            requestId);
      }
      return mergeSessionStatsIntoResult(brokerResult, session, expectedByStage);
    } catch (Exception ex) {
      return tryRecoverWithStream(session, expectedByStage, deadlineMs, ex);
    } catch (Throwable e) {
      session.fanOutCancel();
      throw e;
    } finally {
      if (statsManager != null) {
        for (QueryServerInstance server : incrementedServers) {
          statsManager.recordStatsUponResponseArrival(requestId, server.getInstanceId(), -1);
        }
      }
      if (isQueryCancellationEnabled()) {
        _serversByQuery.remove(requestId);
      }
    }
  }

  /// Streaming variant of {@link #submit}: opens one {@code SubmitWithStream} bidi RPC per server, registers each
  /// open stream with {@code session} (so cancel fan-out and {@code OpChainComplete} accumulation work), and waits
  /// for every server's submit-ack before returning. Errors during ack-await trigger {@link #cancel} on all peers.
  @VisibleForTesting
  void submitWithStream(long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Set<QueryServerInstance> serversOut, Map<String, String> queryOptions, StreamingQuerySession session)
      throws Exception {
    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);

    Set<DispatchablePlanFragment> plansWithoutRoot = dispatchableSubPlan.getQueryStagesWithoutRoot();
    Map<DispatchablePlanFragment, StageInfo> stageInfos = serializePlanFragments(plansWithoutRoot, serversOut);
    if (serversOut.isEmpty()) {
      return;
    }

    Map<String, String> requestMetadata =
        prepareRequestMetadata(QueryThreadContext.get().getExecutionContext(), queryOptions, deadline);
    ByteString protoRequestMetadata = QueryPlanSerDeUtils.toProtoProperties(requestMetadata);

    // Per-server expected opchain count = sum across the server's non-root stages of (workers on this server in
    // that stage). The streaming observer uses this to drain the session latch correctly when its stream errors
    // before all opchains have responded.
    BlockingQueue<AsyncResponse<Worker.QueryResponse>> ackQueue = new ArrayBlockingQueue<>(serversOut.size());
    for (QueryServerInstance server : serversOut) {
      Worker.QueryRequest request = createRequest(server, stageInfos, protoRequestMetadata);
      int expectedForServer = 0;
      for (DispatchablePlanFragment stagePlan : plansWithoutRoot) {
        List<Integer> workerIds = stagePlan.getServerInstanceToWorkerIdMap().get(server);
        if (workerIds != null) {
          expectedForServer += workerIds.size();
        }
      }
      DispatchClient client = getOrCreateDispatchClient(server);
      try {
        client.submitWithStream(request, server, deadline, session, expectedForServer,
            (resp, err) -> ackQueue.offer(new AsyncResponse<>(server, resp, err)));
      } catch (Throwable t) {
        // The error ack was already delivered through the observer's onError inside submitWithStream (CAS-deduped
        // against a later gRPC-initiated onError). Offering another one here could double-fill the ack queue —
        // it is sized exactly serversOut.size() and offer() drops silently — losing a healthy server's ack and
        // stalling processResults until the deadline. Only log and mark the server unhealthy.
        LOGGER.warn("Caught exception while opening stream to server: {}", server, t);
        _failureDetector.markServerUnhealthy(server.getInstanceId(), server.getHostname());
      }
    }

    processResults(requestId, serversOut.size(), (response, server) -> {
      if (response.containsMetadata(ServerResponseStatus.STATUS_ERROR)) {
        session.fanOutCancel();
        throw new RuntimeException(
            String.format("Unable to execute query plan for request: %d on server: %s, ERROR: %s", requestId, server,
                response.getMetadataOrDefault(ServerResponseStatus.STATUS_ERROR, "null")));
      }
    }, deadline, ackQueue);

    if (isQueryCancellationEnabled()) {
      _serversByQuery.put(requestId, serversOut);
    }
  }

  /// Builds the final {@link QueryResult} for a stream-mode query: takes the broker's local stage-0 stats from
  /// {@code brokerResult} and overlays the per-stage trees from the session accumulator (flattened to
  /// {@link MultiStageQueryStats.StageStats.Closed} via inorder traversal so the resulting list shape matches the
  /// legacy {@link QueryResult#_queryStats} contract).
  ///
  /// In stream mode the broker's local mailbox path is suppressed for stages 1..N, so brokerResult's _queryStats
  /// list typically only contains stage 0 plus any pipeline-breaker stages. The session's accumulator carries
  /// stages 1..N. Where both have an entry for the same stage id, the session wins (avoids double-counting
  /// pipeline-breaker stats that the upstream server also reported).
  ///
  /// @param expectedByStage map from stage id to the number of opchain reports expected for that stage (used to
  ///                        compute the {@link QueryResult.StageCoverage#getMissing()} count per stage)
  private QueryResult mergeSessionStatsIntoResult(QueryResult brokerResult, StreamingQuerySession session,
      Map<Integer, Integer> expectedByStage) {
    StreamingQuerySession.Coverage coverage = session.snapshotCoverage();
    Map<Integer, StageStatsTreeNode> accumulator = coverage.getStageAccumulator();

    // Bound the result-array size by the broker's own stage count and the stages the broker actually dispatched
    // (expectedByStage) only — never by the server-supplied accumulator keys. Otherwise a buggy/mismatched server
    // reporting a huge currentStageId would force a giant ArrayList allocation here. Accumulator entries beyond this
    // bound are skipped by the loop below (it only iterates 0..maxStageId).
    int maxStageId = brokerResult.getQueryStats().size() - 1;
    for (Integer stageId : expectedByStage.keySet()) {
      if (stageId > maxStageId) {
        maxStageId = stageId;
      }
    }
    int maxStageIdBound = maxStageId;
    long unexpectedStages = accumulator.keySet().stream().filter(id -> id > maxStageIdBound).count();
    if (unexpectedStages > 0) {
      LOGGER.warn("Ignoring stream stats for {} unexpected stage id(s) on request {} (max expected stage {})",
          unexpectedStages, session.getRequestId(), maxStageIdBound);
    }

    List<MultiStageQueryStats.StageStats.Closed> merged = new ArrayList<>(maxStageId + 1);
    List<QueryResult.StageCoverage> stageCoverage = new ArrayList<>(maxStageId + 1);
    for (int i = 0; i <= maxStageId; i++) {
      StageStatsTreeNode sessionTree = accumulator.get(i);
      if (sessionTree != null) {
        merged.add(sessionTree.flattenInorder());
      } else if (i < brokerResult.getQueryStats().size()) {
        merged.add(brokerResult.getQueryStats().get(i));
      } else {
        merged.add(null);
      }
      int responded = coverage.getRespondedByStage().getOrDefault(i, 0);
      int mergeFailed = coverage.getMergeFailedByStage().getOrDefault(i, 0);
      int expected = expectedByStage.getOrDefault(i, 0);
      int missing = Math.max(0, expected - responded - mergeFailed);
      // Stage 0 is broker-local and not tracked by the session; leave its entry null.
      stageCoverage.add(expected == 0 ? null : new QueryResult.StageCoverage(responded, mergeFailed, missing));
    }
    return new QueryResult(brokerResult.getResultTable(), brokerResult.getProcessingException(), merged,
        brokerResult.getBrokerReduceTimeMs(), stageCoverage, accumulator);
  }

  /// Tries to recover from an exception thrown during legacy (non-streaming) query dispatching.
  ///
  /// [QueryException] and [TimeoutException] are handled by returning a [QueryResult] with the error code and empty
  /// stats, while other exceptions are directly rethrown. Stats are not collected on the legacy cancel path.
  ///
  /// <b>Why {@code cancelWithStats} was removed:</b> a previous revision of this method called a synchronous
  /// {@code cancelWithStats} RPC on every participating server (fan-out) to collect partial per-stage stats on the
  /// error path. That approach was reverted for two reasons:
  /// <ol>
  ///   <li><b>Cascade risk.</b> At high QPS, every query failure triggered an extra fan-out RPC to every server that
  ///       already handled the failed query. Servers under stress would receive a second wave of requests just as they
  ///       were trying to recover, risking a cascading overload.</li>
  ///   <li><b>No consumer.</b> The call site that used the returned {@code Map<Integer, StageStats.Closed>} was also
  ///       reverted as part of the same change, leaving the RPC overhead with no benefit.</li>
  /// </ol>
  ///
  /// Stats on the error path are now available only in stream mode ({@code SubmitWithStream}), where servers push
  /// {@code OpChainComplete} messages independently and the broker collects whatever arrives before the drain
  /// timeout (see {@link #tryRecoverWithStream}).
  private QueryResult tryRecover(long requestId, Set<QueryServerInstance> servers, Exception ex)
      throws Exception {
    if (servers.isEmpty()) {
      throw ex;
    }
    if (ex instanceof ExecutionException && ex.getCause() instanceof Exception) {
      ex = (Exception) ex.getCause();
    }
    QueryErrorCode errorCode;
    if (ex instanceof TimeoutException) {
      errorCode = QueryErrorCode.EXECUTION_TIMEOUT;
    } else if (ex instanceof QueryException) {
      errorCode = ((QueryException) ex).getErrorCode();
    } else {
      cancel(requestId, servers);
      throw ex;
    }
    LOGGER.warn("Query failed with a known exception. Cancelling remaining opchains.");
    cancel(requestId, servers);
    QueryProcessingException processingException = new QueryProcessingException(errorCode, ex.getMessage());
    return new QueryResult(processingException, MultiStageQueryStats.emptyStats(0), 0L);
  }

  /// Tries to recover from an exception thrown during stream-mode ({@code SubmitWithStream}) query dispatching.
  ///
  /// Fans out cancel over the open streams, waits briefly for any remaining {@code OpChainComplete} messages (up to
  /// the query deadline), and builds a {@link QueryResult} that includes whatever stats arrived before the deadline.
  /// Stats from before the error are available because servers push {@code OpChainComplete} even on failure.
  ///
  /// Unknown exceptions (not {@link TimeoutException} or {@link QueryException}) are re-thrown after cancel fan-out.
  private QueryResult tryRecoverWithStream(StreamingQuerySession session, Map<Integer, Integer> expectedByStage,
      long deadlineMs, Exception ex)
      throws Exception {
    if (ex instanceof ExecutionException && ex.getCause() instanceof Exception) {
      ex = (Exception) ex.getCause();
    }
    QueryErrorCode errorCode;
    if (ex instanceof TimeoutException) {
      errorCode = QueryErrorCode.EXECUTION_TIMEOUT;
    } else if (ex instanceof QueryException) {
      errorCode = ((QueryException) ex).getErrorCode();
    } else {
      session.fanOutCancel();
      throw ex;
    }
    LOGGER.warn("Stream-mode query failed with a known exception. Fanning out cancel and waiting for stats.");
    session.fanOutCancel();
    // Cap the wait: the query result is already determined, so we collect stats on a best-effort basis only.
    // Using the full remaining timeout here would regress error-path latency to the query deadline.
    long statsWaitMs = Math.min(_statsDrainMs, Math.max(0, deadlineMs - System.currentTimeMillis()));
    try {
      session.awaitCompletion(statsWaitMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      // Restore the interrupt flag but continue: mergeSessionStatsIntoResult does not block, so the flag will be
      // observed by the caller rather than firing an unexpected InterruptedException inside this method.
      Thread.currentThread().interrupt();
    }
    QueryProcessingException processingException = new QueryProcessingException(errorCode, ex.getMessage());
    QueryResult errorResult = new QueryResult(processingException, MultiStageQueryStats.emptyStats(0), 0L);
    return mergeSessionStatsIntoResult(errorResult, session, expectedByStage);
  }

  public List<PlanNode> explain(RequestContext context, DispatchablePlanFragment fragment, long timeoutMs,
      Map<String, String> queryOptions)
      throws TimeoutException, InterruptedException, ExecutionException {
    long requestId = context.getRequestId();
    List<PlanNode> planNodes = new ArrayList<>();

    Set<DispatchablePlanFragment> plans = Set.of(fragment);
    Set<QueryServerInstance> servers = new HashSet<>();
    try {
      SendRequest<Worker.QueryRequest, List<Worker.ExplainResponse>> requestSender = DispatchClient::explain;
      execute(requestId, plans, timeoutMs, queryOptions, requestSender, servers, (responses, serverInstance) -> {
        for (Worker.ExplainResponse response : responses) {
          if (response.containsMetadata(ServerResponseStatus.STATUS_ERROR)) {
            cancel(requestId, servers);
            throw new RuntimeException(
                String.format("Unable to explain query plan for request: %d on server: %s, ERROR: %s", requestId,
                    serverInstance, response.getMetadataOrDefault(ServerResponseStatus.STATUS_ERROR, "null")));
          }
          for (Worker.StagePlan stagePlan : response.getStagePlanList()) {
            try {
              ByteString rootNode = stagePlan.getRootNode();
              Plan.PlanNode planNode = Plan.PlanNode.parseFrom(rootNode);
              planNodes.add(PlanNodeDeserializer.process(planNode));
            } catch (InvalidProtocolBufferException e) {
              cancel(requestId, servers);
              throw new RuntimeException(
                  "Failed to parse explain plan node for request " + requestId + " from server " + serverInstance, e);
            }
          }
        }
      });
    } catch (Throwable e) {
      // TODO: Consider always cancel when it returns (early terminate)
      cancel(requestId, servers);
      throw e;
    }
    return planNodes;
  }

  @VisibleForTesting
  void submit(long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Set<QueryServerInstance> serversOut, Map<String, String> queryOptions)
      throws Exception {
    SendRequest<Worker.QueryRequest, Worker.QueryResponse> requestSender = DispatchClient::submit;
    Set<DispatchablePlanFragment> plansWithoutRoot = dispatchableSubPlan.getQueryStagesWithoutRoot();
    execute(requestId, plansWithoutRoot, timeoutMs, queryOptions, requestSender, serversOut,
        (response, serverInstance) -> {
          if (response.containsMetadata(ServerResponseStatus.STATUS_ERROR)) {
            cancel(requestId, serversOut);
            throw new RuntimeException(
                String.format("Unable to execute query plan for request: %d on server: %s, ERROR: %s", requestId,
                    serverInstance, response.getMetadataOrDefault(ServerResponseStatus.STATUS_ERROR, "null")));
          }
        });
    if (isQueryCancellationEnabled()) {
      _serversByQuery.put(requestId, serversOut);
    }
  }

  public FailureDetector.ServerState checkConnectivityToInstance(ServerInstance serverInstance) {
    String hostname = serverInstance.getHostname();
    int port = serverInstance.getQueryServicePort();

    DispatchClient client = _dispatchClientMap.get(toHostnamePortKey(hostname, port));
    // Could occur if the cluster is only serving single-stage queries
    if (client == null) {
      LOGGER.debug("No DispatchClient found for server with instanceId: {}", serverInstance.getInstanceId());
      return FailureDetector.ServerState.UNKNOWN;
    }

    ConnectivityState connectivityState = client.getChannel().getState(true);
    if (connectivityState == ConnectivityState.READY) {
      LOGGER.info("Successfully connected to server: {}", serverInstance.getInstanceId());
      return FailureDetector.ServerState.HEALTHY;
    } else {
      LOGGER.info("Still can't connect to server: {}, current state: {}", serverInstance.getInstanceId(),
          connectivityState);
      return FailureDetector.ServerState.UNHEALTHY;
    }
  }

  private boolean isQueryCancellationEnabled() {
    return _serversByQuery != null;
  }

  private <E> void execute(long requestId, Set<DispatchablePlanFragment> stagePlans, long timeoutMs,
      Map<String, String> queryOptions, SendRequest<Worker.QueryRequest, E> sendRequest,
      Set<QueryServerInstance> serverInstancesOut, BiConsumer<E, QueryServerInstance> resultConsumer)
      throws ExecutionException, InterruptedException, TimeoutException {
    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);

    Map<DispatchablePlanFragment, StageInfo> stageInfos = serializePlanFragments(stagePlans, serverInstancesOut);
    if (serverInstancesOut.isEmpty()) {
      return;
    }

    Map<String, String> requestMetadata =
        prepareRequestMetadata(QueryThreadContext.get().getExecutionContext(), queryOptions, deadline);
    ByteString protoRequestMetadata = QueryPlanSerDeUtils.toProtoProperties(requestMetadata);

    // Submit the query plan to all servers in parallel
    BlockingQueue<AsyncResponse<E>> dispatchCallbacks = dispatch(sendRequest, serverInstancesOut, deadline,
        serverInstance -> createRequest(serverInstance, stageInfos, protoRequestMetadata));

    processResults(requestId, serverInstancesOut.size(), resultConsumer, deadline, dispatchCallbacks);
  }

  private <R, E> BlockingQueue<AsyncResponse<E>> dispatch(SendRequest<R, E> sendRequest,
      Set<QueryServerInstance> serverInstancesOut, Deadline deadline, Function<QueryServerInstance, R> requestBuilder) {
    BlockingQueue<AsyncResponse<E>> dispatchCallbacks = new ArrayBlockingQueue<>(serverInstancesOut.size());

    for (QueryServerInstance serverInstance : serverInstancesOut) {
      Consumer<AsyncResponse<E>> callbackConsumer = response -> {
        if (!dispatchCallbacks.offer(response)) {
          LOGGER.warn("Failed to offer response to dispatchCallbacks queue for query on server: {}", serverInstance);
        }
      };
      R request = requestBuilder.apply(serverInstance);
      DispatchClient dispatchClient = getOrCreateDispatchClient(serverInstance);

      try {
        sendRequest.send(dispatchClient, request, serverInstance, deadline, callbackConsumer);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while dispatching query to server: {}", serverInstance, t);
        callbackConsumer.accept(new AsyncResponse<>(serverInstance, null, t));
        _failureDetector.markServerUnhealthy(serverInstance.getInstanceId(), serverInstance.getHostname());
      }
    }
    return dispatchCallbacks;
  }

  private <E> void processResults(long requestId, int numServers, BiConsumer<E, QueryServerInstance> resultConsumer,
      Deadline deadline, BlockingQueue<AsyncResponse<E>> dispatchCallbacks)
      throws InterruptedException, TimeoutException {
    int numSuccessCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && numSuccessCalls < numServers) {
      AsyncResponse<E> resp =
          dispatchCallbacks.poll(Math.max(1, deadline.timeRemaining(TimeUnit.MILLISECONDS)), TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          // If it's a connectivity issue between the broker and the server, mark the server as unhealthy to prevent
          // subsequent query failures
          if (getOrCreateDispatchClient(resp.getServerInstance()).getChannel().getState(false)
              != ConnectivityState.READY) {
            _failureDetector.markServerUnhealthy(resp.getServerInstance().getInstanceId(),
                resp.getServerInstance().getHostname());
          }
          throw new RuntimeException(
              String.format("Error dispatching query: %d to server: %s", requestId, resp.getServerInstance()),
              resp.getThrowable());
        } else {
          E response = resp.getResponse();
          assert response != null;
          resultConsumer.accept(response, resp.getServerInstance());
          numSuccessCalls++;
        }
      } else {
        LOGGER.info("No response from server for query");
      }
    }
    if (deadline.isExpired()) {
      throw new TimeoutException("Timed out waiting for response of async query-dispatch");
    }
  }

  private static Worker.QueryRequest createRequest(QueryServerInstance serverInstance,
      Map<DispatchablePlanFragment, StageInfo> stageInfos, ByteString protoRequestMetadata) {
    Worker.QueryRequest.Builder requestBuilder = Worker.QueryRequest.newBuilder();
    requestBuilder.setVersion(PlanVersions.V1);

    for (Map.Entry<DispatchablePlanFragment, StageInfo> entry : stageInfos.entrySet()) {
      DispatchablePlanFragment stagePlan = entry.getKey();
      List<Integer> workerIds = stagePlan.getServerInstanceToWorkerIdMap().get(serverInstance);
      if (workerIds != null) { // otherwise this server doesn't need to execute this stage
        List<WorkerMetadata> stageWorkerMetadataList = stagePlan.getWorkerMetadataList();
        List<WorkerMetadata> workerMetadataList = new ArrayList<>(workerIds.size());
        for (int workerId : workerIds) {
          workerMetadataList.add(stageWorkerMetadataList.get(workerId));
        }
        List<Worker.WorkerMetadata> protoWorkerMetadataList =
            QueryPlanSerDeUtils.toProtoWorkerMetadataList(workerMetadataList);
        StageInfo stageInfo = entry.getValue();

        Worker.StagePlan requestStagePlan = Worker.StagePlan.newBuilder()
            .setRootNode(stageInfo._rootNode)
            .setStageMetadata(Worker.StageMetadata.newBuilder()
                .setStageId(stagePlan.getPlanFragment().getFragmentId())
                .addAllWorkerMetadata(protoWorkerMetadataList)
                .setCustomProperty(stageInfo._customProperty)
                .build())
            .build();
        requestBuilder.addStagePlan(requestStagePlan);
      }
    }
    requestBuilder.setMetadata(protoRequestMetadata);
    return requestBuilder.build();
  }

  private static Map<String, String> prepareRequestMetadata(QueryExecutionContext executionContext,
      Map<String, String> queryOptions, Deadline deadline) {
    Map<String, String> requestMetadata = new HashMap<>(queryOptions);
    requestMetadata.put(MetadataKeys.REQUEST_ID, Long.toString(executionContext.getRequestId()));
    requestMetadata.put(MetadataKeys.CORRELATION_ID, executionContext.getCid());
    requestMetadata.put(QueryOptionKey.TIMEOUT_MS, Long.toString(deadline.timeRemaining(TimeUnit.MILLISECONDS)));
    requestMetadata.put(QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS,
        Long.toString(executionContext.getPassiveDeadlineMs() - executionContext.getActiveDeadlineMs()));
    return requestMetadata;
  }

  private Map<DispatchablePlanFragment, StageInfo> serializePlanFragments(Set<DispatchablePlanFragment> stagePlans,
      Set<QueryServerInstance> serverInstances)
      throws InterruptedException, ExecutionException {
    List<CompletableFuture<Pair<DispatchablePlanFragment, StageInfo>>> stageInfoFutures =
        new ArrayList<>(stagePlans.size());
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      serverInstances.addAll(stagePlan.getServerInstanceToWorkerIdMap().keySet());
      stageInfoFutures.add(
          CompletableFuture.supplyAsync(() -> Pair.of(stagePlan, serializePlanFragment(stagePlan)), _executorService));
    }
    Map<DispatchablePlanFragment, StageInfo> stageInfos = Maps.newHashMapWithExpectedSize(stagePlans.size());
    try {
      for (CompletableFuture<Pair<DispatchablePlanFragment, StageInfo>> future : stageInfoFutures) {
        Pair<DispatchablePlanFragment, StageInfo> pair = future.get();
        stageInfos.put(pair.getKey(), pair.getValue());
      }
    } finally {
      for (CompletableFuture<?> future : stageInfoFutures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
    return stageInfos;
  }

  private static StageInfo serializePlanFragment(DispatchablePlanFragment stagePlan) {
    ByteString rootNode = PlanNodeSerializer.process(stagePlan.getPlanFragment().getFragmentRoot()).toByteString();
    ByteString customProperty = QueryPlanSerDeUtils.toProtoProperties(stagePlan.getCustomProperties());
    return new StageInfo(rootNode, customProperty);
  }

  private static class StageInfo {
    final ByteString _rootNode;
    final ByteString _customProperty;

    private StageInfo(ByteString rootNode, ByteString customProperty) {
      _rootNode = rootNode;
      _customProperty = customProperty;
    }
  }

  public boolean cancel(long requestId) {
    if (isQueryCancellationEnabled()) {
      return cancel(requestId, _serversByQuery.remove(requestId));
    } else {
      return false;
    }
  }

  ///  Cancels a request without waiting for the stats in the response.
  private boolean cancel(long requestId, @Nullable Set<QueryServerInstance> servers) {
    if (servers == null) {
      return false;
    }
    for (QueryServerInstance queryServerInstance : servers) {
      try {
        getOrCreateDispatchClient(queryServerInstance).cancelAsync(requestId);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while cancelling query: {} on server: {}", requestId, queryServerInstance, t);
      }
    }
    if (isQueryCancellationEnabled()) {
      _serversByQuery.remove(requestId);
    }
    return true;
  }

  private DispatchClient getOrCreateDispatchClient(QueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    return _dispatchClientMap.computeIfAbsent(toHostnamePortKey(hostname, port),
        k -> new DispatchClient(hostname, port, _tlsConfig, _clientGrpcSslContext, _keepAliveConfig));
  }

  /**
   * Reset the connection backoff for a server. When the GRPC channel enters a TRANSIENT_FAILURE state from
   * connection failures, it will fast fail requests and reconnect with exponential backoff. This method
   * resets the backoff so servers that have recovered can be reconnected to immediately.
   */
  public void resetClientConnectionBackoff(ServerInstance serverInstance) {
    String hostname = serverInstance.getHostname();
    int port = serverInstance.getQueryServicePort();
    DispatchClient dispatchClient = _dispatchClientMap.get(toHostnamePortKey(hostname, port));
    if (dispatchClient != null) {
      LOGGER.info("Resetting connection backoff for server: {}", serverInstance.getInstanceId());
      dispatchClient.getChannel().resetConnectBackoff();
    }
  }

  private static String toHostnamePortKey(String hostname, int port) {
    return String.format("%s_%d", hostname, port);
  }

  @Nullable
  private static SslContext initClientSslContext(@Nullable TlsConfig tlsConfig) {
    if (tlsConfig == null) {
      return null;
    }
    BrokerContext brokerContext = BrokerContext.getInstance();
    SslContext sslContext = brokerContext.getClientGrpcSslContext();
    if (sslContext != null) {
      return sslContext;
    }
    SslContext built = ServerGrpcQueryClient.buildSslContext(tlsConfig);
    brokerContext.setClientGrpcSslContext(built);
    return built;
  }
  /// Concatenates the results of the sub-plan and returns a [QueryResult] with the concatenated result.
  /// [QueryThreadContext] must already be set up before calling this method.
  @VisibleForTesting
  public static QueryResult runReducer(DispatchableSubPlan subPlan, Map<String, String> queryOptions,
      MailboxService mailboxService) {
    long startTimeMs = System.currentTimeMillis();
    // NOTE: Reduce stage is always stage 0
    DispatchablePlanFragment stagePlan = subPlan.getQueryStageMap().get(0);
    PlanFragment planFragment = stagePlan.getPlanFragment();
    PlanNode rootNode = planFragment.getFragmentRoot();
    List<WorkerMetadata> workerMetadata = stagePlan.getWorkerMetadataList();
    Preconditions.checkState(workerMetadata.size() == 1, "Expecting single worker for reduce stage, got: %s",
        workerMetadata.size());

    StageMetadata stageMetadata = new StageMetadata(0, workerMetadata, stagePlan.getCustomProperties());
    OpChainExecutionContext opChainExecutionContext =
        OpChainExecutionContext.fromQueryContext(mailboxService, queryOptions, stageMetadata, workerMetadata.get(0),
            null, true, true);

    PairList<Integer, String> resultFields = subPlan.getQueryResultFields();
    DataSchema sourceSchema = rootNode.getDataSchema();
    int numColumns = resultFields.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Map.Entry<Integer, String> field = resultFields.get(i);
      columnNames[i] = field.getValue();
      columnTypes[i] = sourceSchema.getColumnDataType(field.getKey());
    }
    DataSchema resultSchema = new DataSchema(columnNames, columnTypes);

    ArrayList<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    MultiStageQueryStats queryStats;
    try (OpChain opChain = OpChainConverterDispatcher.convert(rootNode, opChainExecutionContext, (a, b) -> {
    })) {
      MultiStageOperator rootOperator = opChain.getRoot();
      block = rootOperator.nextBlock();
      while (block.isData()) {
        MseBlock.Data dataBlock = (MseBlock.Data) block;
        if (dataBlock.isSerialized()) {
          reduceSerialized(dataBlock.asSerialized(), resultRows, numColumns, resultFields, columnTypes);
        } else {
          reduceRowHeap(dataBlock.asRowHeap(), resultRows, numColumns, resultFields, columnTypes);
        }
        block = rootOperator.nextBlock();
      }
      queryStats = rootOperator.calculateStats();
    }
    // TODO: Improve the error handling, e.g. return partial response
    if (block.isError()) {
      ErrorMseBlock errorBlock = (ErrorMseBlock) block;
      Map<QueryErrorCode, String> queryExceptions = errorBlock.getErrorMessages();

      String errorMessage;
      Map.Entry<QueryErrorCode, String> error;
      String from;
      if (errorBlock.getStageId() >= 0) {
        from = " from stage " + errorBlock.getStageId();
        if (errorBlock.getServerId() != null) {
          from += " on " + errorBlock.getServerId();
        }
      } else {
        from = "";
      }
      if (queryExceptions.size() == 1) {
        error = queryExceptions.entrySet().iterator().next();
        errorMessage = "Received 1 error" + from + ": " + error.getValue();
      } else {
        error = queryExceptions.entrySet().stream().max(QueryDispatcher::compareErrors).orElseThrow();
        errorMessage =
            "Received " + queryExceptions.size() + " errors" + from + ". " + "The one with highest priority is: "
                + error.getValue();
      }
      QueryProcessingException processingEx = new QueryProcessingException(error.getKey().getId(), errorMessage);
      return new QueryResult(processingEx, queryStats, System.currentTimeMillis() - startTimeMs);
    }
    assert block.isSuccess();
    return new QueryResult(new ResultTable(resultSchema, resultRows), queryStats,
        System.currentTimeMillis() - startTimeMs);
  }

  private static void reduceSerialized(SerializedDataBlock block, ArrayList<Object[]> resultRows, int numColumns,
      PairList<Integer, String> resultFields, ColumnDataType[] columnTypes) {
    DataBlock dataBlock = block.getDataBlock();
    if (dataBlock.getNumberOfRows() > 0) {
      List<Object[]> rawRows = DataBlockExtractUtils.extractRows(dataBlock);
      toExternalList(resultRows, numColumns, resultFields, columnTypes, rawRows);
    }
  }

  private static void reduceRowHeap(RowHeapDataBlock block, ArrayList<Object[]> resultRows, int numColumns,
      PairList<Integer, String> resultFields, ColumnDataType[] columnTypes) {
    List<Object[]> rows = block.getRows();
    if (!rows.isEmpty()) {
      toExternalList(resultRows, numColumns, resultFields, columnTypes, rows);
    }
  }

  private static void toExternalList(ArrayList<Object[]> resultRows, int numColumns,
      PairList<Integer, String> resultFields, ColumnDataType[] columnTypes, List<Object[]> rows) {
    resultRows.ensureCapacity(resultRows.size() + rows.size());
    for (Object[] rawRow : rows) {
      Object[] row = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object rawValue = rawRow[resultFields.get(i).getKey()];
        if (rawValue != null) {
          ColumnDataType dataType = columnTypes[i];
          row[i] = dataType.format(dataType.toExternal(rawValue));
        }
      }
      resultRows.add(row);
    }
  }

  // TODO: Improve the way the errors are compared
  private static int compareErrors(Map.Entry<QueryErrorCode, String> entry1, Map.Entry<QueryErrorCode, String> entry2) {
    QueryErrorCode errorCode1 = entry1.getKey();
    QueryErrorCode errorCode2 = entry2.getKey();
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION) {
      return 1;
    }
    if (errorCode2 == QueryErrorCode.QUERY_VALIDATION) {
      return -1;
    }
    return Integer.compare(errorCode1.getId(), errorCode2.getId());
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
    _mailboxService.shutdown();
    _executorService.shutdown();
  }

  public static class QueryResult {
    @Nullable
    private final ResultTable _resultTable;
    @Nullable
    private final QueryProcessingException _processingException;
    private final List<MultiStageQueryStats.StageStats.Closed> _queryStats;
    private final long _brokerReduceTimeMs;
    /**
     * Non-null only in stream-mode queries. Indexed by stage id; entries may be null for stages with no coverage data
     * (e.g. stage 0 which runs broker-local and is not tracked by the session).
     */
    @Nullable
    private final List<StageCoverage> _stageCoverage;
    /**
     * Non-null only in stream-mode queries: the explicit per-stage stats trees decoded from the
     * {@code SubmitWithStream} reports, keyed by stage id. Carries the exact tree shape (including plugin-defined
     * operator types), which the flat {@link #_queryStats} list cannot represent. Stage 0 (broker-local) and stages
     * that never reported are absent.
     */
    @Nullable
    private final Map<Integer, StageStatsTreeNode> _stageStatsTrees;

    /**
     * Creates a successful query result.
     */
    public QueryResult(ResultTable resultTable, MultiStageQueryStats queryStats, long brokerReduceTimeMs) {
      _resultTable = resultTable;
      Preconditions.checkArgument(queryStats.getCurrentStageId() == 0, "Expecting query stats for stage 0, got: %s",
          queryStats.getCurrentStageId());
      int numStages = queryStats.getMaxStageId() + 1;
      _queryStats = new ArrayList<>(numStages);
      _queryStats.add(queryStats.getCurrentStats().close());
      for (int i = 1; i < numStages; i++) {
        _queryStats.add(queryStats.getUpstreamStageStats(i));
      }
      _brokerReduceTimeMs = brokerReduceTimeMs;
      _processingException = null;
      _stageCoverage = null;
      _stageStatsTrees = null;
    }

    /**
     * Creates a failed query result.
     * @param processingException the exception that occurred during query processing
     * @param queryStats the query stats, which may be empty
     */
    public QueryResult(QueryProcessingException processingException, MultiStageQueryStats queryStats,
        long brokerReduceTimeMs) {
      _processingException = processingException;
      _resultTable = null;
      _brokerReduceTimeMs = brokerReduceTimeMs;
      Preconditions.checkArgument(queryStats.getCurrentStageId() == 0, "Expecting query stats for stage 0, got: %s",
          queryStats.getCurrentStageId());
      int numStages = queryStats.getMaxStageId() + 1;
      _queryStats = new ArrayList<>(numStages);
      _queryStats.add(queryStats.getCurrentStats().close());
      for (int i = 1; i < numStages; i++) {
        _queryStats.add(queryStats.getUpstreamStageStats(i));
      }
      _stageCoverage = null;
      _stageStatsTrees = null;
    }

    /**
     * Creates a query result from a pre-built per-stage stats list. Used by the {@code SubmitWithStream} path so
     * the caller can merge the broker's local stage-0 stats with the session accumulator's stages 1..N before
     * constructing the result.
     */
    public QueryResult(@Nullable ResultTable resultTable, @Nullable QueryProcessingException processingException,
        List<MultiStageQueryStats.StageStats.Closed> queryStats, long brokerReduceTimeMs,
        @Nullable List<StageCoverage> stageCoverage, @Nullable Map<Integer, StageStatsTreeNode> stageStatsTrees) {
      _resultTable = resultTable;
      _processingException = processingException;
      _queryStats = queryStats;
      _brokerReduceTimeMs = brokerReduceTimeMs;
      _stageCoverage = stageCoverage;
      _stageStatsTrees = stageStatsTrees;
    }

    @Nullable
    public ResultTable getResultTable() {
      return _resultTable;
    }

    @Nullable
    public QueryProcessingException getProcessingException() {
      return _processingException;
    }

    public List<MultiStageQueryStats.StageStats.Closed> getQueryStats() {
      return _queryStats;
    }

    public long getBrokerReduceTimeMs() {
      return _brokerReduceTimeMs;
    }

    /**
     * Returns per-stage coverage data from the stream-mode session, or {@code null} when the query ran in legacy mode.
     * The list is indexed by stage id; entries may be {@code null} for stages with no coverage info (e.g. stage 0).
     */
    @Nullable
    public List<StageCoverage> getStageCoverage() {
      return _stageCoverage;
    }

    /**
     * Returns the explicit per-stage stats trees decoded from stream-mode reports, keyed by stage id, or {@code null}
     * when the query ran in legacy mode. Stages that never reported are absent from the map.
     */
    @Nullable
    public Map<Integer, StageStatsTreeNode> getStageStatsTrees() {
      return _stageStatsTrees;
    }

    /**
     * Per-stage stats coverage for a stream-mode query. Captures how many opchain reports the broker received vs.
     * expected, and how many it couldn't merge (version-skew or shape mismatch).
     */
    public static final class StageCoverage {
      private final int _responded;
      private final int _mergeFailed;
      private final int _missing;

      public StageCoverage(int responded, int mergeFailed, int missing) {
        _responded = responded;
        _mergeFailed = mergeFailed;
        _missing = missing;
      }

      /** Opchains that reported and whose stats were merged successfully. */
      public int getResponded() {
        return _responded;
      }

      /** Opchains that reported but whose stats the broker could not merge (shape mismatch / decode error). */
      public int getMergeFailed() {
        return _mergeFailed;
      }

      /** Opchains that were expected but never reported (timed out or stream error before reporting). */
      public int getMissing() {
        return _missing;
      }
    }
  }

  private interface SendRequest<R, E> {
    void send(DispatchClient dispatchClient, R request, QueryServerInstance serverInstance, Deadline deadline,
        Consumer<AsyncResponse<E>> callbackConsumer);
  }
}
