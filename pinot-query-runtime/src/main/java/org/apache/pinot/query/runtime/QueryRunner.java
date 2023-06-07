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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.executor.RoundRobinScheduler;
import org.apache.pinot.query.runtime.operator.LeafStageTransferableBlockOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerExecutor;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryRunner} accepts a {@link DistributedStagePlan} and runs it.
 */
public class QueryRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final String PINOT_V1_SERVER_QUERY_CONFIG_PREFIX = "pinot.server.query.executor";

  // This is a temporary before merging the 2 type of executor.
  private ServerQueryExecutorV1Impl _serverExecutor;
  private HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private MailboxService _mailboxService;
  private String _hostname;
  private int _port;

  private ExecutorService _queryWorkerIntermExecutorService;
  private ExecutorService _queryWorkerLeafExecutorService;

  private OpChainSchedulerService _scheduler;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager, HelixManager helixManager,
      ServerMetrics serverMetrics) {
    String instanceName = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME);
    _hostname = instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceName.substring(
        CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceName;
    _port = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, QueryConfig.DEFAULT_QUERY_RUNNER_PORT);
    _helixManager = helixManager;
    try {
      long releaseMs = config.getProperty(QueryConfig.KEY_OF_SCHEDULER_RELEASE_TIMEOUT_MS,
          QueryConfig.DEFAULT_SCHEDULER_RELEASE_TIMEOUT_MS);
      _queryWorkerIntermExecutorService = Executors.newCachedThreadPool(
          new NamedThreadFactory("query_intermediate_worker_on_" + _port + "_port"));
      _queryWorkerLeafExecutorService = Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS,
          new NamedThreadFactory("query_leaf_worker_on_" + _port + "_port"));
      _scheduler = new OpChainSchedulerService(new RoundRobinScheduler(releaseMs),
          getQueryWorkerIntermExecutorService());
      _mailboxService = new MailboxService(_hostname, _port, config, _scheduler::onDataAvailable);
      _serverExecutor = new ServerQueryExecutorV1Impl();
      _serverExecutor.init(config.subset(PINOT_V1_SERVER_QUERY_CONFIG_PREFIX), instanceDataManager, serverMetrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void start()
      throws TimeoutException {
    _helixPropertyStore = _helixManager.getHelixPropertyStore();
    _mailboxService.start();
    _serverExecutor.start();
    _scheduler.startAsync().awaitRunning(30, TimeUnit.SECONDS);
  }

  public void shutDown()
      throws TimeoutException {
    _serverExecutor.shutDown();
    _mailboxService.shutdown();
    _scheduler.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
  }

  /**
   * Execute a {@link DistributedStagePlan}.
   *
   * <p>This execution entry point should be asynchronously called by the request handler and caller should not wait
   * for results/exceptions.</p>
   */
  public void processQuery(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap)
      throws Exception {
    long requestId = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS));
    boolean isTraceEnabled =
        Boolean.parseBoolean(requestMetadataMap.getOrDefault(CommonConstants.Broker.Request.TRACE, "false"));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;

    // run pre-stage execution for all pipeline breakers
    PipelineBreakerResult pipelineBreakerResult;
    try {
      pipelineBreakerResult = PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService,
          distributedStagePlan, timeoutMs, deadlineMs, requestId, isTraceEnabled);
    } catch (Exception e) {
      LOGGER.error("Error executing pre-stage pipeline breaker for: {}:{}", requestId,
          distributedStagePlan.getStageId(), e);
      _scheduler.cancel(requestId);
      throw e;
    }

    // run OpChain
    if (DistributedStagePlan.isLeafStage(distributedStagePlan)) {
      try {
        OpChain rootOperator = compileLeafStage(distributedStagePlan, requestMetadataMap, pipelineBreakerResult,
            timeoutMs, deadlineMs, requestId);
        _scheduler.register(rootOperator);
      } catch (Exception e) {
        LOGGER.error("Error executing leaf stage for: {}:{}", requestId, distributedStagePlan.getStageId(), e);
        _scheduler.cancel(requestId);
        throw e;
      }
    } else {
      try {
        PlanNode stageRoot = distributedStagePlan.getStageRoot();
        OpChain rootOperator = PhysicalPlanVisitor.walkPlanNode(stageRoot,
            new PhysicalPlanContext(_mailboxService, requestId, stageRoot.getPlanFragmentId(), timeoutMs, deadlineMs,
                distributedStagePlan.getServer(), distributedStagePlan.getStageMetadata(), null, isTraceEnabled));
        _scheduler.register(rootOperator);
      } catch (Exception e) {
        LOGGER.error("Error executing intermediate stage for: {}:{}", requestId, distributedStagePlan.getStageId(), e);
        _scheduler.cancel(requestId);
        throw e;
      }
    }
  }

  public void cancel(long requestId) {
    _scheduler.cancel(requestId);
  }

  @VisibleForTesting
  public ExecutorService getQueryWorkerLeafExecutorService() {
    return _queryWorkerLeafExecutorService;
  }

  @VisibleForTesting
  public ExecutorService getQueryWorkerIntermExecutorService() {
    return _queryWorkerIntermExecutorService;
  }

  private OpChain compileLeafStage(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap,
      PipelineBreakerResult pipelineBreakerResult, long timeoutMs, long deadlineMs, long requestId) {
    boolean isTraceEnabled =
        Boolean.parseBoolean(requestMetadataMap.getOrDefault(CommonConstants.Broker.Request.TRACE, "false"));
    List<ServerPlanRequestContext> serverPlanRequestContexts =
        constructServerQueryRequests(distributedStagePlan, requestMetadataMap, pipelineBreakerResult,
            _helixPropertyStore, _mailboxService, deadlineMs);
    List<ServerQueryRequest> serverQueryRequests = new ArrayList<>(serverPlanRequestContexts.size());
    for (ServerPlanRequestContext requestContext : serverPlanRequestContexts) {
      serverQueryRequests.add(new ServerQueryRequest(requestContext.getInstanceRequest(),
          new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), System.currentTimeMillis()));
    }
    MailboxSendNode sendNode = (MailboxSendNode) distributedStagePlan.getStageRoot();
    OpChainExecutionContext opChainExecutionContext =
        new OpChainExecutionContext(_mailboxService, requestId, sendNode.getPlanFragmentId(),
            distributedStagePlan.getServer(), timeoutMs, deadlineMs, distributedStagePlan.getStageMetadata(),
            isTraceEnabled);
    MultiStageOperator leafStageOperator =
        new LeafStageTransferableBlockOperator(opChainExecutionContext, this::processServerQueryRequest,
            serverQueryRequests, sendNode.getDataSchema());
    MailboxSendOperator mailboxSendOperator =
        new MailboxSendOperator(opChainExecutionContext, leafStageOperator, sendNode.getExchangeType(),
            sendNode.getPartitionKeySelector(), sendNode.getCollationKeys(), sendNode.getCollationDirections(),
            sendNode.isSortOnSender(), sendNode.getReceiverStageId());
    return new OpChain(opChainExecutionContext, mailboxSendOperator, Collections.emptyList());
  }

  private static List<ServerPlanRequestContext> constructServerQueryRequests(DistributedStagePlan distributedStagePlan,
      Map<String, String> requestMetadataMap, PipelineBreakerResult pipelineBreakerResult,
      ZkHelixPropertyStore<ZNRecord> helixPropertyStore, MailboxService mailboxService, long deadlineMs) {
    StageMetadata stageMetadata = distributedStagePlan.getStageMetadata();
    WorkerMetadata workerMetadata = distributedStagePlan.getCurrentWorkerMetadata();
    String rawTableName = StageMetadata.getTableName(stageMetadata);
    Map<String, List<String>> tableToSegmentListMap = WorkerMetadata.getTableSegmentsMap(workerMetadata);
    List<ServerPlanRequestContext> requests = new ArrayList<>();
    for (Map.Entry<String, List<String>> tableEntry : tableToSegmentListMap.entrySet()) {
      String tableType = tableEntry.getKey();
      // ZkHelixPropertyStore extends from ZkCacheBaseDataAccessor so it should not cause too much out-of-the-box
      // network traffic. but there's chance to improve this:
      // TODO: use TableDataManager: it is already getting tableConfig and Schema when processing segments.
      if (TableType.OFFLINE.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        requests.add(ServerPlanRequestUtils.build(mailboxService, distributedStagePlan, requestMetadataMap,
            pipelineBreakerResult, tableConfig, schema, StageMetadata.getTimeBoundary(stageMetadata),
            TableType.OFFLINE, tableEntry.getValue(), deadlineMs));
      } else if (TableType.REALTIME.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        requests.add(ServerPlanRequestUtils.build(mailboxService, distributedStagePlan, requestMetadataMap,
            pipelineBreakerResult, tableConfig, schema, StageMetadata.getTimeBoundary(stageMetadata),
            TableType.REALTIME, tableEntry.getValue(), deadlineMs));
      } else {
        throw new IllegalArgumentException("Unsupported table type key: " + tableType);
      }
    }
    return requests;
  }

  private InstanceResponseBlock processServerQueryRequest(ServerQueryRequest request) {
    InstanceResponseBlock result;
    try {
      result = _serverExecutor.execute(request, getQueryWorkerLeafExecutorService());
    } catch (Exception e) {
      InstanceResponseBlock errorResponse = new InstanceResponseBlock();
      errorResponse.getExceptions().put(QueryException.QUERY_EXECUTION_ERROR_CODE,
          e.getMessage() + QueryException.getTruncatedStackTrace(e));
      result = errorResponse;
    }
    return result;
  }
}
