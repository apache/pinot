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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.query.MseWorkerThreadContext;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.RoutingInfo;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.LeafStageTransferableBlockOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerExecutor;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestUtils;
import org.apache.pinot.query.runtime.timeseries.PhysicalTimeSeriesServerPlanVisitor;
import org.apache.pinot.query.runtime.timeseries.TimeSeriesExecutionContext;
import org.apache.pinot.query.runtime.timeseries.serde.TimeSeriesBlockSerde;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.executor.HardLimitExecutor;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.tsdb.planner.TimeSeriesPlanConstants.WorkerRequestMetadataKeys;
import org.apache.pinot.tsdb.planner.TimeSeriesPlanConstants.WorkerResponseMetadataKeys;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.serde.TimeSeriesPlanSerde;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryRunner} accepts a {@link StagePlan} and runs it.
 */
public class QueryRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);

  private String _hostname;
  private int _port;
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
  private Integer _mseMinGroupTrimSize;

  @Nullable
  private Integer _maxInitialResultHolderCapacity;
  @Nullable
  private Integer _minInitialIndexedTableCapacity;
  @Nullable
  private Integer _mseMaxInitialResultHolderCapacity;

  // Join overflow settings
  @Nullable
  private Integer _maxRowsInJoin;
  @Nullable
  private JoinOverFlowMode _joinOverflowMode;
  @Nullable
  private PhysicalTimeSeriesServerPlanVisitor _timeSeriesPhysicalPlanVisitor;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager, HelixManager helixManager,
      ServerMetrics serverMetrics, @Nullable TlsConfig tlsConfig) {
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (hostname.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
      hostname = hostname.substring(CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH);
    }
    int port = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        CommonConstants.MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT);
    _hostname = hostname;
    _port = port;
    _helixManager = helixManager;
    _serverMetrics = serverMetrics;

    // TODO: Consider using separate config for intermediate stage and leaf stage
    String numGroupsLimitStr = config.getProperty(Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT);
    _numGroupsLimit = numGroupsLimitStr != null ? Integer.parseInt(numGroupsLimitStr) : null;

    String mseMinGroupTrimSizeStr = config.getProperty(Server.CONFIG_OF_MSE_MIN_GROUP_TRIM_SIZE);
    _mseMinGroupTrimSize = mseMinGroupTrimSizeStr != null ? Integer.parseInt(mseMinGroupTrimSizeStr) : null;

    String maxInitialGroupHolderCapacity =
        config.getProperty(Server.CONFIG_OF_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _maxInitialResultHolderCapacity =
        maxInitialGroupHolderCapacity != null ? Integer.parseInt(maxInitialGroupHolderCapacity) : null;

    String minInitialIndexedTableCapacityStr =
        config.getProperty(Server.CONFIG_OF_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY);
    _minInitialIndexedTableCapacity =
        minInitialIndexedTableCapacityStr != null ? Integer.parseInt(minInitialIndexedTableCapacityStr) : null;

    String mseMaxInitialGroupHolderCapacity =
        config.getProperty(Server.CONFIG_OF_MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _mseMaxInitialResultHolderCapacity =
        mseMaxInitialGroupHolderCapacity != null ? Integer.parseInt(mseMaxInitialGroupHolderCapacity) : null;

    String maxRowsInJoinStr = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_ROWS_IN_JOIN);
    _maxRowsInJoin = maxRowsInJoinStr != null ? Integer.parseInt(maxRowsInJoinStr) : null;

    String joinOverflowModeStr = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_JOIN_OVERFLOW_MODE);
    _joinOverflowMode = joinOverflowModeStr != null ? JoinOverFlowMode.valueOf(joinOverflowModeStr) : null;

    _executorService = MseWorkerThreadContext.contextAwareExecutorService(
        QueryThreadContext.contextAwareExecutorService(
            ExecutorServiceUtils.create(
                config,
                Server.MULTISTAGE_EXECUTOR_CONFIG_PREFIX, "query-runner-on-" + port,
                Server.DEFAULT_MULTISTAGE_EXECUTOR_TYPE
            )
        )
    );

    int hardLimit = HardLimitExecutor.getMultiStageExecutorHardLimit(config);
    if (hardLimit > 0) {
      _executorService = new HardLimitExecutor(hardLimit, _executorService);
    }

    _opChainScheduler = new OpChainSchedulerService(_executorService);
    _mailboxService = new MailboxService(hostname, port, config, tlsConfig);
    try {
      _leafQueryExecutor = new ServerQueryExecutorV1Impl();
      _leafQueryExecutor.init(config.subset(Server.QUERY_EXECUTOR_CONFIG_PREFIX), instanceDataManager,
          serverMetrics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (StringUtils.isNotBlank(config.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey()))) {
      _timeSeriesPhysicalPlanVisitor = new PhysicalTimeSeriesServerPlanVisitor(_leafQueryExecutor, _executorService,
          serverMetrics);
      TimeSeriesBuilderFactoryProvider.init(config);
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
   * Execute a {@link StagePlan}.
   *
   * <p>This execution entry point should be asynchronously called by the request handler and caller should not wait
   * for results/exceptions.</p>
   */
  public void processQuery(WorkerMetadata workerMetadata, StagePlan stagePlan, Map<String, String> requestMetadata,
      @Nullable ThreadExecutionContext parentContext) {
    long requestId = Long.parseLong(requestMetadata.get(MetadataKeys.REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadata.get(QueryOptionKey.TIMEOUT_MS));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;

    StageMetadata stageMetadata = stagePlan.getStageMetadata();
    Map<String, String> opChainMetadata = consolidateMetadata(stageMetadata.getCustomProperties(), requestMetadata);

    // run pre-stage execution for all pipeline breakers
    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_opChainScheduler, _mailboxService, workerMetadata, stagePlan,
            opChainMetadata, requestId, deadlineMs, parentContext);

    // Send error block to all the receivers if pipeline breaker fails
    if (pipelineBreakerResult != null && pipelineBreakerResult.getErrorBlock() != null) {
      TransferableBlock errorBlock = pipelineBreakerResult.getErrorBlock();
      int stageId = stageMetadata.getStageId();
      LOGGER.error("Error executing pipeline breaker for request: {}, stage: {}, sending error block: {}", requestId,
          stageId, errorBlock.getExceptions());
      MailboxSendNode rootNode = (MailboxSendNode) stagePlan.getRootNode();
      List<RoutingInfo> routingInfos = new ArrayList<>();
      for (Integer receiverStageId : rootNode.getReceiverStageIds()) {
        List<MailboxInfo> receiverMailboxInfos =
            workerMetadata.getMailboxInfosMap().get(receiverStageId).getMailboxInfos();
        List<RoutingInfo> stageRoutingInfos =
            MailboxIdUtils.toRoutingInfos(requestId, stageId, workerMetadata.getWorkerId(), receiverStageId,
                receiverMailboxInfos);
        routingInfos.addAll(stageRoutingInfos);
      }
      for (RoutingInfo routingInfo : routingInfos) {
        try {
          StatMap<MailboxSendOperator.StatKey> statMap = new StatMap<>(MailboxSendOperator.StatKey.class);
          _mailboxService.getSendingMailbox(routingInfo.getHostname(), routingInfo.getPort(),
              routingInfo.getMailboxId(), deadlineMs, statMap).send(errorBlock);
        } catch (TimeoutException e) {
          LOGGER.warn("Timed out sending error block to mailbox: {} for request: {}, stage: {}",
              routingInfo.getMailboxId(), requestId, stageId, e);
        } catch (Exception e) {
          LOGGER.error("Caught exception sending error block to mailbox: {} for request: {}, stage: {}",
              routingInfo.getMailboxId(), requestId, stageId, e);
        }
      }
      return;
    }

    // run OpChain
    OpChainExecutionContext executionContext =
        new OpChainExecutionContext(_mailboxService, requestId, deadlineMs, opChainMetadata, stageMetadata,
            workerMetadata, pipelineBreakerResult, parentContext);
    OpChain opChain;
    if (workerMetadata.isLeafStageWorker()) {
      opChain = ServerPlanRequestUtils.compileLeafStage(executionContext, stagePlan, _helixManager, _serverMetrics,
          _leafQueryExecutor, _executorService);
    } else {
      opChain = PlanNodeToOpChain.convert(stagePlan.getRootNode(), executionContext);
    }
    _opChainScheduler.register(opChain);
  }

  /**
   * Receives a serialized plan sent by the broker, and runs it to completion, blocking the thread until the execution
   * is complete.
   * TODO: This design is at odds with MSE because MSE runs even the leaf stage via OpChainSchedulerService.
   *   However, both OpChain scheduler and this method use the same ExecutorService.
   */
  public void processTimeSeriesQuery(List<String> serializedPlanFragments, Map<String, String> metadata,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    // Define a common way to handle errors.
    final Consumer<Pair<Throwable, String>> handleErrors = (pair) -> {
      Throwable t = pair.getLeft();
      try {
        String planId = pair.getRight();
        Map<String, String> errorMetadata = new HashMap<>();
        errorMetadata.put(WorkerResponseMetadataKeys.ERROR_TYPE, t.getClass().getSimpleName());
        errorMetadata.put(WorkerResponseMetadataKeys.ERROR_MESSAGE, t.getMessage() == null
            ? "Unknown error: no message" : t.getMessage());
        errorMetadata.put(WorkerResponseMetadataKeys.PLAN_ID, planId);
        // TODO(timeseries): remove logging for failed queries.
        LOGGER.warn("time-series query failed:", t);
        responseObserver.onNext(Worker.TimeSeriesResponse.newBuilder().putAllMetadata(errorMetadata).build());
        responseObserver.onCompleted();
      } catch (Throwable t2) {
        LOGGER.warn("Unable to send error to broker. Original error: {}", t.getMessage(), t2);
      }
    };
    if (serializedPlanFragments.isEmpty()) {
      handleErrors.accept(Pair.of(new IllegalStateException("No plan fragments received in server"), ""));
      return;
    }
    try {
      final long deadlineMs = extractDeadlineMs(metadata);
      Preconditions.checkState(System.currentTimeMillis() < deadlineMs,
          "Query timed out before getting processed in server. Exceeded time by (ms): %s",
          System.currentTimeMillis() - deadlineMs);
      List<BaseTimeSeriesPlanNode> fragmentRoots = serializedPlanFragments.stream()
          .map(TimeSeriesPlanSerde::deserialize).collect(Collectors.toList());
      TimeSeriesExecutionContext context = new TimeSeriesExecutionContext(
          metadata.get(WorkerRequestMetadataKeys.LANGUAGE), extractTimeBuckets(metadata), deadlineMs, metadata,
          extractPlanToSegmentMap(metadata), Collections.emptyMap());
      final List<BaseTimeSeriesOperator> fragmentOpChains = fragmentRoots.stream().map(x -> {
        return _timeSeriesPhysicalPlanVisitor.compile(x, context);
      }).collect(Collectors.toList());
      // Run the operator using the same executor service as OpChainSchedulerService
      _executorService.submit(() -> {
        String currentPlanId = "";
        try {
          for (int index = 0; index < fragmentOpChains.size(); index++) {
            currentPlanId = fragmentRoots.get(index).getId();
            BaseTimeSeriesOperator fragmentOpChain = fragmentOpChains.get(index);
            TimeSeriesBlock seriesBlock = fragmentOpChain.nextBlock();
            Worker.TimeSeriesResponse response = Worker.TimeSeriesResponse.newBuilder()
                .setPayload(TimeSeriesBlockSerde.serializeTimeSeriesBlock(seriesBlock))
                .putAllMetadata(ImmutableMap.of(WorkerResponseMetadataKeys.PLAN_ID, currentPlanId))
                .build();
            responseObserver.onNext(response);
          }
          responseObserver.onCompleted();
        } catch (Throwable t) {
          handleErrors.accept(Pair.of(t, currentPlanId));
        }
      });
    } catch (Throwable t) {
      LOGGER.error("Error running time-series query", t);
      handleErrors.accept(Pair.of(t, ""));
    }
  }

  private Map<String, String> consolidateMetadata(Map<String, String> customProperties,
      Map<String, String> requestMetadata) {
    Map<String, String> opChainMetadata = new HashMap<>();
    // 1. put all request level metadata
    opChainMetadata.putAll(requestMetadata);
    // 2. put all stageMetadata.customProperties.
    opChainMetadata.putAll(customProperties);
    // 3. add all overrides from config if anything is still empty.
    Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(opChainMetadata);
    if (numGroupsLimit == null) {
      numGroupsLimit = _numGroupsLimit;
    }
    if (numGroupsLimit != null) {
      opChainMetadata.put(QueryOptionKey.NUM_GROUPS_LIMIT, Integer.toString(numGroupsLimit));
    }

    Integer mseMinGroupTrimSize = QueryOptionsUtils.getMSEMinGroupTrimSize(opChainMetadata);
    if (mseMinGroupTrimSize == null) {
      mseMinGroupTrimSize = _mseMinGroupTrimSize;
    }
    if (mseMinGroupTrimSize != null) {
      opChainMetadata.put(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, Integer.toString(mseMinGroupTrimSize));
    }

    Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(opChainMetadata);
    if (maxInitialResultHolderCapacity == null) {
      maxInitialResultHolderCapacity = _maxInitialResultHolderCapacity;
    }
    if (maxInitialResultHolderCapacity != null) {
      opChainMetadata.put(QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          Integer.toString(maxInitialResultHolderCapacity));
    }

    Integer minInitialIndexedTableCapacity = QueryOptionsUtils.getMinInitialIndexedTableCapacity(opChainMetadata);
    if (minInitialIndexedTableCapacity == null) {
      minInitialIndexedTableCapacity = _minInitialIndexedTableCapacity;
    }
    if (minInitialIndexedTableCapacity != null) {
      opChainMetadata.put(QueryOptionKey.MIN_INITIAL_INDEXED_TABLE_CAPACITY,
          Integer.toString(minInitialIndexedTableCapacity));
    }

    Integer mseMaxInitialResultHolderCapacity = QueryOptionsUtils.getMSEMaxInitialResultHolderCapacity(opChainMetadata);
    if (mseMaxInitialResultHolderCapacity == null) {
      mseMaxInitialResultHolderCapacity = _mseMaxInitialResultHolderCapacity;
    }
    if (mseMaxInitialResultHolderCapacity != null) {
      opChainMetadata.put(QueryOptionKey.MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          Integer.toString(mseMaxInitialResultHolderCapacity));
    }

    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    if (maxRowsInJoin == null) {
      maxRowsInJoin = _maxRowsInJoin;
    }
    if (maxRowsInJoin != null) {
      opChainMetadata.put(QueryOptionKey.MAX_ROWS_IN_JOIN, Integer.toString(maxRowsInJoin));
    }

    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(opChainMetadata);
    if (joinOverflowMode == null) {
      joinOverflowMode = _joinOverflowMode;
    }
    if (joinOverflowMode != null) {
      opChainMetadata.put(QueryOptionKey.JOIN_OVERFLOW_MODE, joinOverflowMode.name());
    }
    return opChainMetadata;
  }

  public void cancel(long requestId) {
    _opChainScheduler.cancel(requestId);
  }

  public StagePlan explainQuery(
      WorkerMetadata workerMetadata, StagePlan stagePlan, Map<String, String> requestMetadata) {

    if (!workerMetadata.isLeafStageWorker()) {
      LOGGER.debug("Explain query on intermediate stages is a NOOP");
      return stagePlan;
    }
    long requestId = Long.parseLong(requestMetadata.get(MetadataKeys.REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadata.get(QueryOptionKey.TIMEOUT_MS));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;

    StageMetadata stageMetadata = stagePlan.getStageMetadata();
    Map<String, String> opChainMetadata = consolidateMetadata(stageMetadata.getCustomProperties(), requestMetadata);

    if (PipelineBreakerExecutor.hasPipelineBreakers(stagePlan)) {
      // TODO: Support pipeline breakers before merging this feature.
      //  See https://github.com/apache/pinot/pull/13733#discussion_r1752031714
      LOGGER.error("Pipeline breaker is not supported in explain query");
      return stagePlan;
    }

    Map<PlanNode, ExplainedNode> leafNodes = new HashMap<>();
    BiConsumer<PlanNode, MultiStageOperator> leafNodesConsumer = (node, operator) -> {
      if (operator instanceof LeafStageTransferableBlockOperator) {
        LeafStageTransferableBlockOperator leafOperator = (LeafStageTransferableBlockOperator) operator;
        ExplainedNode explainedNode = leafOperator.explain();
        leafNodes.put(node, explainedNode);
      }
    };
    // compile OpChain
    OpChainExecutionContext executionContext = new OpChainExecutionContext(_mailboxService, requestId, deadlineMs,
        opChainMetadata, stageMetadata, workerMetadata, null, null);

    OpChain opChain = ServerPlanRequestUtils.compileLeafStage(executionContext, stagePlan, _helixManager,
        _serverMetrics, _leafQueryExecutor, _executorService, leafNodesConsumer, true);
    opChain.close(); // probably unnecessary, but formally needed

    PlanNode rootNode = substituteNode(stagePlan.getRootNode(), leafNodes);

    return new StagePlan(rootNode, stagePlan.getStageMetadata());
  }

  private PlanNode substituteNode(PlanNode node, Map<PlanNode, ? extends PlanNode> substitutions) {
    if (substitutions.containsKey(node)) {
      return substitutions.get(node);
    }
    List<PlanNode> oldInputs = node.getInputs();
    List<PlanNode> newInputs = new ArrayList<>(oldInputs.size());
    boolean requiresNewNode = false;
    for (PlanNode oldInput : oldInputs) {
      PlanNode newInput = substituteNode(oldInput, substitutions);
      newInputs.add(newInput);
      if (oldInput != newInput) {
        requiresNewNode = true;
      }
    }
    if (requiresNewNode) {
      return node.withInputs(newInputs);
    } else {
      return node;
    }
  }

  // Time series related utility methods below

  private long extractDeadlineMs(Map<String, String> metadataMap) {
    return Long.parseLong(metadataMap.get(WorkerRequestMetadataKeys.DEADLINE_MS));
  }

  private TimeBuckets extractTimeBuckets(Map<String, String> metadataMap) {
    long startTimeSeconds = Long.parseLong(metadataMap.get(WorkerRequestMetadataKeys.START_TIME_SECONDS));
    long windowSeconds = Long.parseLong(metadataMap.get(WorkerRequestMetadataKeys.WINDOW_SECONDS));
    int numElements = Integer.parseInt(metadataMap.get(WorkerRequestMetadataKeys.NUM_ELEMENTS));
    return TimeBuckets.ofSeconds(startTimeSeconds, Duration.ofSeconds(windowSeconds), numElements);
  }

  private Map<String, List<String>> extractPlanToSegmentMap(Map<String, String> metadataMap) {
    Map<String, List<String>> result = new HashMap<>();
    for (var entry : metadataMap.entrySet()) {
      if (WorkerRequestMetadataKeys.isKeySegmentList(entry.getKey())) {
        String planId = WorkerRequestMetadataKeys.decodeSegmentListKey(entry.getKey());
        String[] segments = entry.getValue().split(",");
        result.put(planId,
            Stream.of(segments).map(String::strip).collect(Collectors.toList()));
      }
    }
    return result;
  }
}
