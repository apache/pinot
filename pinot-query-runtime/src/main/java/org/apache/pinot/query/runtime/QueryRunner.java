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
package org.apache.pinot.query.runtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.executor.ExecutorServiceUtils;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.LeafStageTransferableBlockOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerExecutor;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryRunner} accepts a {@link DistributedStagePlan} and runs it.
 */
public class QueryRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);

  private HelixManager _helixManager;
  private ServerMetrics _serverMetrics;

  private ExecutorService _executorService;
  private OpChainSchedulerService _opChainScheduler;
  private MailboxService _mailboxService;
  private QueryExecutor _leafQueryExecutor;

  // Group-by settings
  @Nullable
  private Integer _numGroupsLimit;
  @Nullable
  private Integer _maxInitialResultHolderCapacity;

  // Join overflow settings
  @Nullable
  private Integer _maxRowsInJoin;
  @Nullable
  private JoinOverFlowMode _joinOverflowMode;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager, HelixManager helixManager,
      ServerMetrics serverMetrics) {
    _helixManager = helixManager;
    _serverMetrics = serverMetrics;

    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (hostname.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
      hostname = hostname.substring(CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH);
    }
    int port = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT);

    // TODO: Consider using separate config for intermediate stage and leaf stage
    String numGroupsLimitStr = config.getProperty(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT);
    _numGroupsLimit = numGroupsLimitStr != null ? Integer.parseInt(numGroupsLimitStr) : null;
    String maxInitialGroupHolderCapacity =
        config.getProperty(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _maxInitialResultHolderCapacity =
        maxInitialGroupHolderCapacity != null ? Integer.parseInt(maxInitialGroupHolderCapacity) : null;
    String maxRowsInJoinStr = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_ROWS_IN_JOIN);
    _maxRowsInJoin = maxRowsInJoinStr != null ? Integer.parseInt(maxRowsInJoinStr) : null;
    String joinOverflowModeStr = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_JOIN_OVERFLOW_MODE);
    _joinOverflowMode = joinOverflowModeStr != null ? JoinOverFlowMode.valueOf(joinOverflowModeStr) : null;

    //TODO: make this configurable
    _executorService = ExecutorServiceUtils.createDefault("query-runner-on-" + port);
    _opChainScheduler = new OpChainSchedulerService(_executorService);
    _mailboxService = new MailboxService(hostname, port, config);
    try {
      _leafQueryExecutor = new ServerQueryExecutorV1Impl();
      _leafQueryExecutor.init(config.subset(CommonConstants.Server.QUERY_EXECUTOR_CONFIG_PREFIX), instanceDataManager,
          serverMetrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LOGGER.info("Initialized QueryRunner with hostname: {}, port: {}", hostname, port);
  }

  @VisibleForTesting
  public ExecutorService getExecutorService() {
    return _executorService;
  }

  public void start() {
    _mailboxService.start();
    _leafQueryExecutor.start();
  }

  public void shutDown() {
    _leafQueryExecutor.shutDown();
    _mailboxService.shutdown();
    ExecutorServiceUtils.close(_executorService);
  }

  /**
   * Execute a {@link DistributedStagePlan}.
   *
   * <p>This execution entry point should be asynchronously called by the request handler and caller should not wait
   * for results/exceptions.</p>
   */
  public void processQuery(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadata) {
    long requestId = Long.parseLong(requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;

    setStageCustomProperties(distributedStagePlan.getStageMetadata().getCustomProperties(), requestMetadata);

    // run pre-stage execution for all pipeline breakers
    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_opChainScheduler, _mailboxService, distributedStagePlan,
            requestMetadata, requestId, deadlineMs);

    // Send error block to all the receivers if pipeline breaker fails
    if (pipelineBreakerResult != null && pipelineBreakerResult.getErrorBlock() != null) {
      TransferableBlock errorBlock = pipelineBreakerResult.getErrorBlock();
      LOGGER.error("Error executing pipeline breaker for request: {}, stage: {}, sending error block: {}", requestId,
          distributedStagePlan.getStageId(), errorBlock.getExceptions());
      int receiverStageId = ((MailboxSendNode) distributedStagePlan.getStageRoot()).getReceiverStageId();
      MailboxMetadata mailboxMetadata = distributedStagePlan.getStageMetadata().getWorkerMetadataList()
          .get(distributedStagePlan.getServer().workerId()).getMailBoxInfosMap().get(receiverStageId);
      List<String> mailboxIds = MailboxIdUtils.toMailboxIds(requestId, mailboxMetadata);
      for (int i = 0; i < mailboxIds.size(); i++) {
        try {
          _mailboxService.getSendingMailbox(mailboxMetadata.getVirtualAddress(i).hostname(),
              mailboxMetadata.getVirtualAddress(i).port(), mailboxIds.get(i), deadlineMs).send(errorBlock);
        } catch (TimeoutException e) {
          LOGGER.warn("Timed out sending error block to mailbox: {} for request: {}, stage: {}", mailboxIds.get(i),
              requestId, distributedStagePlan.getStageId(), e);
        } catch (Exception e) {
          LOGGER.error("Caught exception sending error block to mailbox: {} for request: {}, stage: {}",
              mailboxIds.get(i), requestId, distributedStagePlan.getStageId(), e);
        }
      }
      return;
    }

    // run OpChain
    OpChainExecutionContext executionContext =
        new OpChainExecutionContext(_mailboxService, requestId, distributedStagePlan.getStageId(),
            distributedStagePlan.getServer(), deadlineMs, requestMetadata, distributedStagePlan.getStageMetadata(),
            pipelineBreakerResult);
    OpChain opChain;
    if (DistributedStagePlan.isLeafStage(distributedStagePlan)) {
      opChain = compileLeafStage(executionContext, distributedStagePlan);
    } else {
      opChain = PhysicalPlanVisitor.walkPlanNode(distributedStagePlan.getStageRoot(), executionContext);
    }
    _opChainScheduler.register(opChain);
  }

  private void setStageCustomProperties(Map<String, String> customProperties, Map<String, String> requestMetadata) {
    Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(requestMetadata);
    if (numGroupsLimit == null) {
      numGroupsLimit = _numGroupsLimit;
    }
    if (numGroupsLimit != null) {
      customProperties.put(QueryOptionKey.NUM_GROUPS_LIMIT, Integer.toString(numGroupsLimit));
    }

    Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(requestMetadata);
    if (maxInitialResultHolderCapacity == null) {
      maxInitialResultHolderCapacity = _maxInitialResultHolderCapacity;
    }
    if (maxInitialResultHolderCapacity != null) {
      customProperties.put(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          Integer.toString(maxInitialResultHolderCapacity));
    }

    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(requestMetadata);
    if (maxRowsInJoin == null) {
      maxRowsInJoin = _maxRowsInJoin;
    }
    if (maxRowsInJoin != null) {
      customProperties.put(QueryOptionKey.MAX_ROWS_IN_JOIN, Integer.toString(maxRowsInJoin));
    }

    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(requestMetadata);
    if (joinOverflowMode == null) {
      joinOverflowMode = _joinOverflowMode;
    }
    if (joinOverflowMode != null) {
      customProperties.put(QueryOptionKey.JOIN_OVERFLOW_MODE, joinOverflowMode.name());
    }
  }

  public void cancel(long requestId) {
    _opChainScheduler.cancel(requestId);
  }

  private OpChain compileLeafStage(OpChainExecutionContext executionContext,
      DistributedStagePlan distributedStagePlan) {
    List<ServerPlanRequestContext> serverPlanRequestContexts =
        ServerPlanRequestUtils.constructServerQueryRequests(executionContext, distributedStagePlan,
            _helixManager.getHelixPropertyStore());
    List<ServerQueryRequest> serverQueryRequests = new ArrayList<>(serverPlanRequestContexts.size());
    long queryArrivalTimeMs = System.currentTimeMillis();
    for (ServerPlanRequestContext requestContext : serverPlanRequestContexts) {
      serverQueryRequests.add(
          new ServerQueryRequest(requestContext.getInstanceRequest(), _serverMetrics, queryArrivalTimeMs, true));
    }
    MailboxSendNode sendNode = (MailboxSendNode) distributedStagePlan.getStageRoot();
    MultiStageOperator leafStageOperator =
        new LeafStageTransferableBlockOperator(executionContext, serverQueryRequests, sendNode.getDataSchema(),
            _leafQueryExecutor, _executorService);
    MailboxSendOperator mailboxSendOperator =
        new MailboxSendOperator(executionContext, leafStageOperator, sendNode.getDistributionType(),
            sendNode.getPartitionKeySelector(), sendNode.getCollationKeys(), sendNode.getCollationDirections(),
            sendNode.isSortOnSender(), sendNode.getReceiverStageId());
    return new OpChain(executionContext, mailboxSendOperator);
  }
}
