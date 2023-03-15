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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
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
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.MultiplexingMailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.executor.RoundRobinScheduler;
import org.apache.pinot.query.runtime.operator.LeafStageTransferableBlockOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.apache.pinot.query.runtime.plan.PlanRequestContext;
import org.apache.pinot.query.runtime.plan.ServerRequestPlanVisitor;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
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
  private MailboxService<TransferableBlock> _mailboxService;
  private String _hostname;
  private int _port;
  private VirtualServerAddress _rootServer;
  // Query worker threads are used for (1) running intermediate stage operators (2) running segment level operators
  /**
   * Query worker threads are used for:
   * <ol>
   *   <li>
   *     Running intermediate stage operators (v2 engine operators).
   *   </li>
   *   <li>
   *     Running per-segment operators submitted in {@link BaseCombineOperator}.
   *   </li>
   * </ol>
   */
  private ExecutorService _queryWorkerExecutorService;
  /**
   * Query runner threads are used for:
   * <ol>
   *   <li>
   *     Merging results in BaseCombineOperator for leaf stages. Results are provided by per-segment operators run in
   *     worker threads
   *   </li>
   *   <li>
   *     Building the OperatorChain and submitting to the scheduler for non-leaf stages (intermediate stages).
   *   </li>
   * </ol>
   */
  private ExecutorService _queryRunnerExecutorService;
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
    // always use 0 for root server ID as all data is processed by one node at the global root
    _rootServer = new VirtualServerAddress(_hostname, _port, 0);
    _helixManager = helixManager;
    try {
      long releaseMs = config.getProperty(QueryConfig.KEY_OF_SCHEDULER_RELEASE_TIMEOUT_MS,
          QueryConfig.DEFAULT_SCHEDULER_RELEASE_TIMEOUT_MS);
      _queryWorkerExecutorService = Executors.newFixedThreadPool(
          ResourceManager.DEFAULT_QUERY_WORKER_THREADS,
          new NamedThreadFactory("query_worker_on_" + _port + "_port"));
      _queryRunnerExecutorService = Executors.newFixedThreadPool(
          ResourceManager.DEFAULT_QUERY_RUNNER_THREADS,
          new NamedThreadFactory("query_runner_on_" + _port + "_port"));
      _scheduler = new OpChainSchedulerService(new RoundRobinScheduler(releaseMs), _queryWorkerExecutorService);
      _mailboxService = MultiplexingMailboxService.newInstance(_hostname, _port, config, _scheduler::onDataAvailable);
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

  public void processQuery(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap) {
    long requestId = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    if (isLeafStage(distributedStagePlan)) {
      runLeafStage(distributedStagePlan, requestMetadataMap, deadlineMs, requestId);
    } else {
      StageNode stageRoot = distributedStagePlan.getStageRoot();
      OpChain rootOperator = PhysicalPlanVisitor.build(stageRoot,
          new PlanRequestContext(_mailboxService, requestId, stageRoot.getStageId(), timeoutMs, deadlineMs,
              new VirtualServerAddress(distributedStagePlan.getServer()), distributedStagePlan.getMetadataMap()));
      _scheduler.register(rootOperator);
    }
  }

  public void cancel(long requestId) {
    _scheduler.cancel(requestId);
  }

  public ExecutorService getQueryWorkerExecutorService() {
    return _queryWorkerExecutorService;
  }

  public ExecutorService getQueryRunnerExecutorService() {
    return _queryRunnerExecutorService;
  }

  private void runLeafStage(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap,
      long deadlineMs, long requestId) {
    // TODO: make server query request return via mailbox, this is a hack to gather the non-streaming data table
    // and package it here for return. But we should really use a MailboxSendOperator directly put into the
    // server executor.
    MailboxSendOperator mailboxSendOperator = null;
    try {
      long leafStageStartMillis = System.currentTimeMillis();
      List<ServerPlanRequestContext> serverQueryRequests =
          constructServerQueryRequests(distributedStagePlan, requestMetadataMap, _helixPropertyStore, _mailboxService,
              deadlineMs);

      // send the data table via mailbox in one-off fashion (e.g. no block-level split, one data table/partition key)
      List<InstanceResponseBlock> serverQueryResults = new ArrayList<>(serverQueryRequests.size());
      for (ServerPlanRequestContext requestContext : serverQueryRequests) {
        ServerQueryRequest request = new ServerQueryRequest(requestContext.getInstanceRequest(),
            new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), System.currentTimeMillis());
        serverQueryResults.add(processServerQuery(request, _scheduler.getWorkerPool()));
      }
      LOGGER.debug(
          "RequestId:" + requestId + " StageId:" + distributedStagePlan.getStageId() + " Leaf stage v1 processing time:"
              + (System.currentTimeMillis() - leafStageStartMillis) + " ms");
      MailboxSendNode sendNode = (MailboxSendNode) distributedStagePlan.getStageRoot();
      OpChainExecutionContext opChainExecutionContext =
          new OpChainExecutionContext(_mailboxService, requestId, sendNode.getStageId(), _rootServer, deadlineMs,
              deadlineMs, distributedStagePlan.getMetadataMap());
      mailboxSendOperator = new MailboxSendOperator(
          new LeafStageTransferableBlockOperator(serverQueryResults, sendNode.getDataSchema(), opChainExecutionContext),
          sendNode.getExchangeType(), sendNode.getPartitionKeySelector(), sendNode.getStageId(),
          sendNode.getReceiverStageId(), opChainExecutionContext);
      int blockCounter = 0;
      while (!TransferableBlockUtils.isEndOfStream(mailboxSendOperator.nextBlock())) {
        LOGGER.debug("Acquired transferable block: {}", blockCounter++);
      }
      mailboxSendOperator.close();
    } catch (Exception e) {
      LOGGER.error(String.format("Error running leafStage for requestId=%s", requestId), e);
      if (mailboxSendOperator != null) {
        mailboxSendOperator.cancel(e);
      }
    }
  }

  private static List<ServerPlanRequestContext> constructServerQueryRequests(DistributedStagePlan distributedStagePlan,
      Map<String, String> requestMetadataMap, ZkHelixPropertyStore<ZNRecord> helixPropertyStore,
      MailboxService<TransferableBlock> mailboxService, long deadlineMs) {
    StageMetadata stageMetadata = distributedStagePlan.getMetadataMap().get(distributedStagePlan.getStageId());
    Preconditions.checkState(stageMetadata.getScannedTables().size() == 1,
        "Server request for V2 engine should only have 1 scan table per request.");
    String rawTableName = stageMetadata.getScannedTables().get(0);
    Map<String, List<String>> tableToSegmentListMap =
        stageMetadata.getServerInstanceToSegmentsMap().get(distributedStagePlan.getServer().getServer());
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
        requests.add(
            ServerRequestPlanVisitor.build(mailboxService, distributedStagePlan, requestMetadataMap, tableConfig,
                schema, stageMetadata.getTimeBoundaryInfo(), TableType.OFFLINE, tableEntry.getValue(), deadlineMs));
      } else if (TableType.REALTIME.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        requests.add(
            ServerRequestPlanVisitor.build(mailboxService, distributedStagePlan, requestMetadataMap, tableConfig,
                schema, stageMetadata.getTimeBoundaryInfo(), TableType.REALTIME, tableEntry.getValue(), deadlineMs));
      } else {
        throw new IllegalArgumentException("Unsupported table type key: " + tableType);
      }
    }
    return requests;
  }

  private InstanceResponseBlock processServerQuery(ServerQueryRequest serverQueryRequest,
      ExecutorService executorService) {
    try {
      return _serverExecutor.execute(serverQueryRequest, executorService);
    } catch (Exception e) {
      InstanceResponseBlock errorResponse = new InstanceResponseBlock();
      errorResponse.getExceptions()
          .put(QueryException.QUERY_EXECUTION_ERROR_CODE, e.getMessage() + QueryException.getTruncatedStackTrace(e));
      return errorResponse;
    }
  }

  private boolean isLeafStage(DistributedStagePlan distributedStagePlan) {
    int stageId = distributedStagePlan.getStageId();
    VirtualServer serverInstance = distributedStagePlan.getServer();
    StageMetadata stageMetadata = distributedStagePlan.getMetadataMap().get(stageId);
    Map<String, List<String>> segments = stageMetadata.getServerInstanceToSegmentsMap().get(serverInstance.getServer());
    return segments != null && segments.size() > 0;
  }
}
