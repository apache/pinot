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
package org.apache.pinot.query.runtime.plan;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.operator.factory.DefaultQueryOperatorFactoryProvider;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 *  The {@code OpChainExecutionContext} class contains the information derived from the PlanRequestContext.
 *  Members of this class should not be changed once initialized.
 *  This information is then used by the OpChain to create the Operators for a query.
 */
public class OpChainExecutionContext {
  private final MailboxService _mailboxService;
  private final long _requestId;
  private final String _cid;
  private final long _activeDeadlineMs;
  private final long _passiveDeadlineMs;
  private final String _brokerId;
  private final Map<String, String> _opChainMetadata;
  private final StageMetadata _stageMetadata;
  private final WorkerMetadata _workerMetadata;
  private final VirtualServerAddress _server;
  private final OpChainId _id;
  @Nullable
  private final PipelineBreakerResult _pipelineBreakerResult;
  private final boolean _traceEnabled;
  private final QueryOperatorFactoryProvider _queryOperatorFactoryProvider;
  @Nullable
  private ServerPlanRequestContext _leafStageContext;
  private final boolean _sendStats;
  private final boolean _keepPipelineBreakerStats;
  /**
   * Map of MultiStageOperator -> PlanNodes that compile down to that operator. Populated by
   * {@link org.apache.pinot.query.runtime.plan.PlanNodeToOpChain} during opchain construction. Cardinality is
   * one-to-many: an intermediate operator maps to a single PlanNode, but the leaf operator maps to the whole sub-tree
   * of v1 plan nodes below the leaf-stage boundary. Used by the stream-mode stats reporting path
   * ({@code MultiStageStatsTreeEncoder}) to attach plan-node identifiers to each operator's stats.
   * <p>
   * Identity-based (IdentityHashMap) because PlanNode equality is structural and two distinct nodes can compare equal.
   * <p>
   * Lazily allocated (only stream-mode stats reporting populates it), so legacy queries — the default — allocate
   * nothing here. Written only during single-threaded opchain construction, read only at stats-report time after the
   * opchain has run, so the lifecycle establishes the happens-before; no volatile/synchronization is needed.
   */
  @Nullable
  private Map<MultiStageOperator, List<PlanNode>> _operatorToPlanNodes;
  /**
   * Stage-scoped plan-node ids: each PlanNode reachable from the opchain's root receives a sequential integer id
   * assigned by a deterministic pre-order walk. Both broker and server perform the same walk over the same plan
   * structure, so the ids match without being serialized on the wire. Used by {@code MultiStageStatsTreeEncoder} to
   * populate {@code StageStatsNode.plan_node_ids}.
   * <p>
   * Identity-based for the same reason as {@link #_operatorToPlanNodes}; lazily allocated for the same reason too.
   */
  @Nullable
  private Map<PlanNode, Integer> _planNodeIds;

  @VisibleForTesting
  public OpChainExecutionContext(MailboxService mailboxService, long requestId, String cid, long activeDeadlineMs,
      long passiveDeadlineMs, String brokerId, Map<String, String> opChainMetadata, StageMetadata stageMetadata,
      WorkerMetadata workerMetadata, @Nullable PipelineBreakerResult pipelineBreakerResult, boolean sendStats,
      boolean keepPipelineBreakerStats) {
    _mailboxService = mailboxService;
    // TODO: Consider removing info included in QueryExecutionContext
    _requestId = requestId;
    _cid = cid;
    _activeDeadlineMs = activeDeadlineMs;
    _passiveDeadlineMs = passiveDeadlineMs;
    _brokerId = brokerId;
    _opChainMetadata = Collections.unmodifiableMap(opChainMetadata);
    _stageMetadata = stageMetadata;
    _workerMetadata = workerMetadata;
    _sendStats = sendStats;
    _server =
        new VirtualServerAddress(mailboxService.getHostname(), mailboxService.getPort(), workerMetadata.getWorkerId());
    _id = new OpChainId(requestId, workerMetadata.getWorkerId(), stageMetadata.getStageId());
    _pipelineBreakerResult = pipelineBreakerResult;
    _traceEnabled = Boolean.parseBoolean(opChainMetadata.get(CommonConstants.Broker.Request.TRACE));
    _queryOperatorFactoryProvider = getDefaultQueryOperatorFactoryProvider();
    _keepPipelineBreakerStats = keepPipelineBreakerStats;
  }

  public static OpChainExecutionContext fromQueryContext(MailboxService mailboxService,
      Map<String, String> opChainMetadata, StageMetadata stageMetadata, WorkerMetadata workerMetadata,
      @Nullable PipelineBreakerResult pipelineBreakerResult, boolean sendStats, boolean keepPipelineBreakerStats) {
    return fromQueryContext(mailboxService, opChainMetadata, stageMetadata, workerMetadata, pipelineBreakerResult,
        sendStats, keepPipelineBreakerStats, QueryThreadContext.get().getExecutionContext());
  }

  @VisibleForTesting
  public static OpChainExecutionContext fromQueryContext(MailboxService mailboxService,
      Map<String, String> opChainMetadata, StageMetadata stageMetadata, WorkerMetadata workerMetadata,
      @Nullable PipelineBreakerResult pipelineBreakerResult, boolean sendStats, boolean keepPipelineBreakerStats,
      QueryExecutionContext queryExecutionContext) {
    return new OpChainExecutionContext(mailboxService, queryExecutionContext.getRequestId(),
        queryExecutionContext.getCid(), queryExecutionContext.getActiveDeadlineMs(),
        queryExecutionContext.getPassiveDeadlineMs(), queryExecutionContext.getBrokerId(), opChainMetadata,
        stageMetadata, workerMetadata, pipelineBreakerResult, sendStats, keepPipelineBreakerStats);
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getCid() {
    return _cid;
  }

  public int getStageId() {
    return _stageMetadata.getStageId();
  }

  public int getWorkerId() {
    return _workerMetadata.getWorkerId();
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  /// Returns the deadline in milliseconds for the OpChain to complete when it is actively waiting for data.
  ///
  /// This deadline should only be used for _active_ waits, like when a
  /// [HashJoinOperator][org.apache.pinot.query.runtime.operator.HashJoinOperator] is building the hash table.
  ///
  /// This should not be used for _passive_ waits, like when the a
  /// [MailboxReceiveOperator][org.apache.pinot.query.runtime.operator.MailboxReceiveOperator] or a
  /// [PipelineBreakerOperator][org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerOperator] passively waits
  /// for data to arrive.
  public long getActiveDeadlineMs() {
    return _activeDeadlineMs;
  }

  /// Returns the deadline in milliseconds for the OpChain to complete when it is passively waiting for data.
  ///
  /// This deadline should only be used for _passive_ waits, like when the
  /// [MailboxReceiveOperator][org.apache.pinot.query.runtime.operator.MailboxReceiveOperator] or a
  /// [PipelineBreakerOperator][org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerOperator]
  /// passively waits for data to arrive.
  ///
  /// This should not be used for _active_ waits, like when the a
  /// [HashJoinOperator][org.apache.pinot.query.runtime.operator.HashJoinOperator] is building the hash table.
  public long getPassiveDeadlineMs() {
    return _passiveDeadlineMs;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public Map<String, String> getOpChainMetadata() {
    return _opChainMetadata;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public WorkerMetadata getWorkerMetadata() {
    return _workerMetadata;
  }

  public OpChainId getId() {
    return _id;
  }

  @Nullable
  public PipelineBreakerResult getPipelineBreakerResult() {
    return _pipelineBreakerResult;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }

  public QueryOperatorFactoryProvider getQueryOperatorFactoryProvider() {
    return _queryOperatorFactoryProvider;
  }

  @Nullable
  public ServerPlanRequestContext getLeafStageContext() {
    return _leafStageContext;
  }

  public void setLeafStageContext(ServerPlanRequestContext leafStageContext) {
    _leafStageContext = leafStageContext;
  }

  public boolean isSendStats() {
    return _sendStats;
  }

  /**
   * Whether stats for this opchain are reported out-of-band via the {@code SubmitWithStream} bidi RPC (stream mode),
   * rather than piggybacked on the mailbox EOS (legacy mode). Set server-side by the {@code SubmitWithStream} handler
   * via {@link CommonConstants.MultiStageQueryRunner#KEY_OF_STATS_REPORTING_MODE}. This is the only mode that consumes
   * the per-operator plan-node ids, so the id pre-walk in {@code PlanNodeToOpChain} is gated on it — gating on
   * {@link #isSendStats()} instead would both waste work on every legacy query (the default) and, conversely, skip
   * the walk in stream mode (where {@code sendStats} is forced off), leaving the ids unpopulated.
   */
  public boolean isStreamStatsReporting() {
    return CommonConstants.MultiStageQueryRunner.STATS_REPORTING_MODE_STREAM.equals(
        _opChainMetadata.get(CommonConstants.MultiStageQueryRunner.KEY_OF_STATS_REPORTING_MODE));
  }

  public boolean isKeepPipelineBreakerStats() {
    return _keepPipelineBreakerStats;
  }

  /**
   * Records which PlanNodes compiled down to the given MultiStageOperator. Should be called once per operator as the
   * opchain is constructed; subsequent calls overwrite. {@code planNodes} is captured by reference; callers should not
   * mutate it after passing it in.
   */
  public void recordPlanNodesForOperator(MultiStageOperator operator, List<PlanNode> planNodes) {
    if (_operatorToPlanNodes == null) {
      _operatorToPlanNodes = new IdentityHashMap<>();
    }
    _operatorToPlanNodes.put(operator, planNodes);
  }

  /**
   * Returns the PlanNodes that compiled down to the given operator, or an empty list if no mapping was recorded.
   */
  public List<PlanNode> getPlanNodesForOperator(MultiStageOperator operator) {
    return _operatorToPlanNodes == null ? List.of() : _operatorToPlanNodes.getOrDefault(operator, List.of());
  }

  /**
   * Records the stage-scoped id assigned to the given PlanNode. Should only be called once per node by the
   * {@link org.apache.pinot.query.runtime.plan.PlanNodeToOpChain} pre-walk.
   */
  public void recordPlanNodeId(PlanNode node, int id) {
    if (_planNodeIds == null) {
      _planNodeIds = new IdentityHashMap<>();
    }
    _planNodeIds.put(node, id);
  }

  /**
   * Returns the stage-scoped id assigned to the given PlanNode, or {@code -1} if no id was recorded for it.
   */
  public int getPlanNodeId(PlanNode node) {
    Integer id = _planNodeIds == null ? null : _planNodeIds.get(node);
    return id != null ? id : -1;
  }

  /**
   * Returns an unmodifiable view of the recorded PlanNode→id assignments, or an empty map when none were recorded
   * (ids are only assigned in stream-stats mode). Useful for plugins that need to resolve PlanNodes by id, e.g. to
   * register synthetic stats operators via {@link #recordPlanNodesForOperator}.
   */
  public Map<PlanNode, Integer> getPlanNodeIdMap() {
    return _planNodeIds == null ? Map.of() : Collections.unmodifiableMap(_planNodeIds);
  }

  private static QueryOperatorFactoryProvider getDefaultQueryOperatorFactoryProvider() {
    // Prefer server context when explicitly configured, otherwise fall back to broker, then default.
    Object serverProvider = ServerContext.getInstance().getQueryOperatorFactoryProvider();
    if (serverProvider instanceof QueryOperatorFactoryProvider) {
      return (QueryOperatorFactoryProvider) serverProvider;
    }
    Object brokerProvider = BrokerContext.getInstance().getQueryOperatorFactoryProvider();
    if (brokerProvider instanceof QueryOperatorFactoryProvider) {
      return (QueryOperatorFactoryProvider) brokerProvider;
    }
    return DefaultQueryOperatorFactoryProvider.INSTANCE;
  }
}
