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
import java.util.ArrayList;
import java.util.Collections;
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
import javax.annotation.Nullable;
import org.apache.calcite.runtime.PairList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.timeseries.PhysicalTimeSeriesBrokerPlanVisitor;
import org.apache.pinot.query.runtime.timeseries.TimeSeriesExecutionContext;
import org.apache.pinot.query.service.dispatch.timeseries.TimeSeriesDispatchClient;
import org.apache.pinot.query.service.dispatch.timeseries.TimeSeriesDispatchObserver;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tsdb.planner.TimeSeriesExchangeNode;
import org.apache.pinot.tsdb.planner.TimeSeriesPlanConstants.WorkerRequestMetadataKeys;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesQueryServerInstance;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);
  private static final String PINOT_BROKER_QUERY_DISPATCHER_FORMAT = "multistage-query-dispatch-%d";

  private final MailboxService _mailboxService;
  private final ExecutorService _executorService;
  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();
  private final Map<String, TimeSeriesDispatchClient> _timeSeriesDispatchClientMap = new ConcurrentHashMap<>();
  @Nullable
  private final TlsConfig _tlsConfig;
  // maps broker-generated query id to the set of servers that the query was dispatched to
  private final Map<Long, Set<QueryServerInstance>> _serversByQuery;
  private final PhysicalTimeSeriesBrokerPlanVisitor _timeSeriesBrokerPlanVisitor
      = new PhysicalTimeSeriesBrokerPlanVisitor();
  private final FailureDetector _failureDetector;

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector) {
    this(mailboxService, failureDetector, null, false);
  }

  public QueryDispatcher(MailboxService mailboxService, FailureDetector failureDetector, @Nullable TlsConfig tlsConfig,
      boolean enableCancellation) {
    _mailboxService = mailboxService;
    _executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
        new TracedThreadFactory(Thread.NORM_PRIORITY, false, PINOT_BROKER_QUERY_DISPATCHER_FORMAT));
    _tlsConfig = tlsConfig;
    _failureDetector = failureDetector;

    if (enableCancellation) {
      _serversByQuery = new ConcurrentHashMap<>();
    } else {
      _serversByQuery = null;
    }
  }

  public void start() {
    _mailboxService.start();
  }

  public QueryResult submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions)
      throws Exception {
    long requestId = context.getRequestId();
    Set<QueryServerInstance> servers = new HashSet<>();
    try {
      submit(requestId, dispatchableSubPlan, timeoutMs, servers, queryOptions);
      try {
        return runReducer(requestId, dispatchableSubPlan, timeoutMs, queryOptions, _mailboxService);
      } finally {
        if (isQueryCancellationEnabled()) {
          _serversByQuery.remove(requestId);
        }
      }
    } catch (Throwable e) {
      // TODO: Consider always cancel when it returns (early terminate)
      cancel(requestId, servers);
      throw e;
    }
  }

  public List<PlanNode> explain(RequestContext context, DispatchablePlanFragment fragment, long timeoutMs,
      Map<String, String> queryOptions)
      throws TimeoutException, InterruptedException, ExecutionException {
    long requestId = context.getRequestId();
    List<PlanNode> planNodes = new ArrayList<>();

    Set<DispatchablePlanFragment> plans = Collections.singleton(fragment);
    Set<QueryServerInstance> servers = new HashSet<>();
    try {
      SendRequest<List<Worker.ExplainResponse>> requestSender = DispatchClient::explain;
      execute(requestId, plans, timeoutMs, queryOptions, requestSender, servers, (responses, serverInstance) -> {
        for (Worker.ExplainResponse response : responses) {
          if (response.containsMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR)) {
            cancel(requestId, servers);
            throw new RuntimeException(
                String.format("Unable to explain query plan for request: %d on server: %s, ERROR: %s", requestId,
                    serverInstance,
                    response.getMetadataOrDefault(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
                        "null")));
          }
          for (Worker.StagePlan stagePlan : response.getStagePlanList()) {
            try {
              ByteString rootNode = stagePlan.getRootNode();
              Plan.PlanNode planNode = Plan.PlanNode.parseFrom(rootNode);
              planNodes.add(PlanNodeDeserializer.process(planNode));
            } catch (InvalidProtocolBufferException e) {
              cancel(requestId, servers);
              throw new RuntimeException("Failed to parse explain plan node for request " + requestId + " from server "
                  + serverInstance, e);
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
  void submit(
      long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs, Set<QueryServerInstance> serversOut,
      Map<String, String> queryOptions)
      throws Exception {
    SendRequest<Worker.QueryResponse> requestSender = DispatchClient::submit;
    Set<DispatchablePlanFragment> plansWithoutRoot = dispatchableSubPlan.getQueryStagesWithoutRoot();
    execute(requestId, plansWithoutRoot, timeoutMs, queryOptions, requestSender, serversOut,
        (response, serverInstance) -> {
      if (response.containsMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR)) {
        cancel(requestId, serversOut);
        throw new RuntimeException(
            String.format("Unable to execute query plan for request: %d on server: %s, ERROR: %s", requestId,
                serverInstance,
                response.getMetadataOrDefault(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
                    "null")));
      }
    });
    if (isQueryCancellationEnabled()) {
      _serversByQuery.put(requestId, serversOut);
    }
  }

  public FailureDetector.ServerState checkConnectivityToInstance(ServerInstance serverInstance) {
    String hostname = serverInstance.getHostname();
    int port = serverInstance.getQueryServicePort();
    String hostnamePort = String.format("%s_%d", hostname, port);

    DispatchClient client = _dispatchClientMap.get(hostnamePort);
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

  private <E> void execute(long requestId, Set<DispatchablePlanFragment> stagePlans,
      long timeoutMs, Map<String, String> queryOptions,
      SendRequest<E> sendRequest, Set<QueryServerInstance> serverInstancesOut,
      BiConsumer<E, QueryServerInstance> resultConsumer)
      throws ExecutionException, InterruptedException, TimeoutException {

    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);

    Map<DispatchablePlanFragment, StageInfo> stageInfos =
        serializePlanFragments(stagePlans, serverInstancesOut, deadline);

    if (serverInstancesOut.isEmpty()) {
      throw new RuntimeException("No server instances to dispatch query to");
    }

    Map<String, String> requestMetadata =
        prepareRequestMetadata(requestId, QueryThreadContext.getCid(), queryOptions, deadline);
    ByteString protoRequestMetadata = QueryPlanSerDeUtils.toProtoProperties(requestMetadata);

    // Submit the query plan to all servers in parallel
    int numServers = serverInstancesOut.size();
    BlockingQueue<AsyncResponse<E>> dispatchCallbacks = new ArrayBlockingQueue<>(numServers);

    for (QueryServerInstance serverInstance : serverInstancesOut) {
      Consumer<AsyncResponse<E>> callbackConsumer = response -> {
        if (!dispatchCallbacks.offer(response)) {
          LOGGER.warn("Failed to offer response to dispatchCallbacks queue for query: {} on server: {}", requestId,
              serverInstance);
        }
      };
      Worker.QueryRequest requestBuilder = createRequest(serverInstance, stageInfos, protoRequestMetadata);
      DispatchClient dispatchClient = getOrCreateDispatchClient(serverInstance);

      try {
        sendRequest.send(dispatchClient, requestBuilder, serverInstance, deadline, callbackConsumer);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while dispatching query: {} to server: {}", requestId, serverInstance, t);
        callbackConsumer.accept(new AsyncResponse<>(serverInstance, null, t));
        _failureDetector.markServerUnhealthy(serverInstance.getInstanceId());
      }
    }

    int numSuccessCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && numSuccessCalls < numServers) {
      AsyncResponse<E> resp =
          dispatchCallbacks.poll(deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          // If it's a connectivity issue between the broker and the server, mark the server as unhealthy to prevent
          // subsequent query failures
          if (getOrCreateDispatchClient(resp.getServerInstance()).getChannel().getState(false)
              != ConnectivityState.READY) {
            _failureDetector.markServerUnhealthy(resp.getServerInstance().getInstanceId());
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
      }
    }
    if (deadline.isExpired()) {
      throw new TimeoutException("Timed out waiting for response of async query-dispatch");
    }
  }

  Map<String, String> initializeTimeSeriesMetadataMap(TimeSeriesDispatchablePlan dispatchablePlan, long deadlineMs,
      RequestContext requestContext, String instanceId) {
    Map<String, String> result = new HashMap<>();
    TimeBuckets timeBuckets = dispatchablePlan.getTimeBuckets();
    result.put(WorkerRequestMetadataKeys.LANGUAGE, dispatchablePlan.getLanguage());
    result.put(WorkerRequestMetadataKeys.START_TIME_SECONDS, Long.toString(timeBuckets.getTimeBuckets()[0]));
    result.put(WorkerRequestMetadataKeys.WINDOW_SECONDS, Long.toString(timeBuckets.getBucketSize().getSeconds()));
    result.put(WorkerRequestMetadataKeys.NUM_ELEMENTS, Long.toString(timeBuckets.getTimeBuckets().length));
    result.put(WorkerRequestMetadataKeys.DEADLINE_MS, Long.toString(deadlineMs));
    Map<String, List<String>> leafIdToSegments = dispatchablePlan.getLeafIdToSegmentsByInstanceId().get(instanceId);
    for (Map.Entry<String, List<String>> entry : leafIdToSegments.entrySet()) {
      result.put(WorkerRequestMetadataKeys.encodeSegmentListKey(entry.getKey()), String.join(",", entry.getValue()));
    }
    result.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestContext.getRequestId()));
    result.put(CommonConstants.Query.Request.MetadataKeys.BROKER_ID, requestContext.getBrokerId());
    return result;
  }

  private static Worker.QueryRequest createRequest(QueryServerInstance serverInstance,
      Map<DispatchablePlanFragment, StageInfo> stageInfos, ByteString protoRequestMetadata) {
    Worker.QueryRequest.Builder requestBuilder = Worker.QueryRequest.newBuilder();
    requestBuilder.setVersion(CommonConstants.MultiStageQueryRunner.PlanVersions.V1);

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
            .setStageMetadata(
                Worker.StageMetadata.newBuilder()
                    .setStageId(stagePlan.getPlanFragment().getFragmentId())
                    .addAllWorkerMetadata(protoWorkerMetadataList)
                    .setCustomProperty(stageInfo._customProperty)
                    .build()
            )
            .build();
        requestBuilder.addStagePlan(requestStagePlan);
      }
    }
    requestBuilder.setMetadata(protoRequestMetadata);
    return requestBuilder.build();
  }

  private static Map<String, String> prepareRequestMetadata(long requestId, String cid,
      Map<String, String> queryOptions, Deadline deadline) {
    Map<String, String> requestMetadata = new HashMap<>();
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestId));
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.CORRELATION_ID, cid);
    requestMetadata.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS,
        Long.toString(deadline.timeRemaining(TimeUnit.MILLISECONDS)));
    requestMetadata.putAll(queryOptions);
    return requestMetadata;
  }

  private Map<DispatchablePlanFragment, StageInfo> serializePlanFragments(
      Set<DispatchablePlanFragment> stagePlans,
      Set<QueryServerInstance> serverInstances, Deadline deadline)
      throws InterruptedException, ExecutionException, TimeoutException {
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

  private boolean cancel(long requestId, @Nullable Set<QueryServerInstance> servers) {
    if (servers == null) {
      return false;
    }
    for (QueryServerInstance queryServerInstance : servers) {
      try {
        getOrCreateDispatchClient(queryServerInstance).cancel(requestId);
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
    String hostnamePort = String.format("%s_%d", hostname, port);
    return _dispatchClientMap.computeIfAbsent(hostnamePort, k -> new DispatchClient(hostname, port, _tlsConfig));
  }

  private TimeSeriesDispatchClient getOrCreateTimeSeriesDispatchClient(
      TimeSeriesQueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    String key = String.format("%s_%d", hostname, port);
    return _timeSeriesDispatchClientMap.computeIfAbsent(key, k -> new TimeSeriesDispatchClient(hostname, port));
  }

  // There is no reduction happening here, results are simply concatenated.
  @VisibleForTesting
  public static QueryResult runReducer(long requestId,
      DispatchableSubPlan subPlan,
      long timeoutMs,
      Map<String, String> queryOptions,
      MailboxService mailboxService) {

    long startTimeMs = System.currentTimeMillis();
    long deadlineMs = startTimeMs + timeoutMs;
    // NOTE: Reduce stage is always stage 0
    DispatchablePlanFragment stagePlan = subPlan.getQueryStageMap().get(0);
    PlanFragment planFragment = stagePlan.getPlanFragment();
    PlanNode rootNode = planFragment.getFragmentRoot();

    Preconditions.checkState(rootNode instanceof MailboxReceiveNode,
        "Expecting mailbox receive node as root of reduce stage, got: %s", rootNode.getClass().getSimpleName());

    MailboxReceiveNode receiveNode = (MailboxReceiveNode) rootNode;
    List<WorkerMetadata> workerMetadata = stagePlan.getWorkerMetadataList();

    Preconditions.checkState(workerMetadata.size() == 1,
        "Expecting single worker for reduce stage, got: %s", workerMetadata.size());

    StageMetadata stageMetadata = new StageMetadata(0, workerMetadata, stagePlan.getCustomProperties());
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    OpChainExecutionContext executionContext =
        new OpChainExecutionContext(mailboxService, requestId, deadlineMs, queryOptions, stageMetadata,
            workerMetadata.get(0), null, parentContext);

    PairList<Integer, String> resultFields = subPlan.getQueryResultFields();
    DataSchema sourceSchema = receiveNode.getDataSchema();
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
    TransferableBlock block;
    try (MailboxReceiveOperator receiveOperator = new MailboxReceiveOperator(executionContext, receiveNode)) {
      block = receiveOperator.nextBlock();
      while (!TransferableBlockUtils.isEndOfStream(block)) {
        DataBlock dataBlock = block.getDataBlock();
        int numRows = dataBlock.getNumberOfRows();
        if (numRows > 0) {
          resultRows.ensureCapacity(resultRows.size() + numRows);
          List<Object[]> rawRows = DataBlockExtractUtils.extractRows(dataBlock);
          for (Object[] rawRow : rawRows) {
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
        block = receiveOperator.nextBlock();
      }
    }
    // TODO: Improve the error handling, e.g. return partial response
    if (block.isErrorBlock()) {
      Map<Integer, String> queryExceptions = block.getExceptions();

      if (queryExceptions.size() == 1) {
        Map.Entry<Integer, String> error = queryExceptions.entrySet().iterator().next();
        QueryErrorCode errorCode = QueryErrorCode.fromErrorCode(error.getKey());
        throw errorCode.asException("Received 1 error from servers: " + error.getValue());
      } else {
        Map.Entry<Integer, String> highestPriorityError = queryExceptions.entrySet().stream()
            .max(QueryDispatcher::compareErrors)
            .orElseThrow();
        throw QueryErrorCode.fromErrorCode(highestPriorityError.getKey())
            .asException("Received " + queryExceptions.size() + " errors from servers. "
                + "The one with highest priority is: " + highestPriorityError.getValue());
      }
    }
    assert block.isSuccessfulEndOfStreamBlock();
    MultiStageQueryStats queryStats = block.getQueryStats();
    assert queryStats != null;
    return new QueryResult(new ResultTable(resultSchema, resultRows), queryStats,
        System.currentTimeMillis() - startTimeMs);
  }

  // TODO: Improve the way the errors are compared
  private static int compareErrors(Map.Entry<Integer, String> entry1, Map.Entry<Integer, String> entry2) {
    int errorCode1 = entry1.getKey();
    int errorCode2 = entry2.getKey();
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION.getId()) {
      return errorCode1;
    }
    if (errorCode2 == QueryErrorCode.QUERY_VALIDATION.getId()) {
      return errorCode2;
    }
    return Integer.compare(errorCode1, errorCode2);
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
    _mailboxService.shutdown();
    _executorService.shutdown();
  }

  public PinotBrokerTimeSeriesResponse submitAndGet(RequestContext context, TimeSeriesDispatchablePlan plan,
      long timeoutMs, Map<String, String> queryOptions) {
    long requestId = context.getRequestId();
    try {
      TimeSeriesBlock result = submitAndGet(requestId, plan, timeoutMs, queryOptions, context);
      return PinotBrokerTimeSeriesResponse.fromTimeSeriesBlock(result);
    } catch (Throwable t) {
      return PinotBrokerTimeSeriesResponse.newErrorResponse(t.getClass().getSimpleName(), t.getMessage());
    }
  }

  TimeSeriesBlock submitAndGet(long requestId, TimeSeriesDispatchablePlan plan, long timeoutMs,
      Map<String, String> queryOptions, RequestContext requestContext)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    BaseTimeSeriesPlanNode brokerFragment = plan.getBrokerFragment();
    // Get consumers for leafs
    Map<String, BlockingQueue<Object>> receiversByPlanId = new HashMap<>();
    populateConsumers(brokerFragment, receiversByPlanId);
    // Compile brokerFragment to get operators
    TimeSeriesExecutionContext brokerExecutionContext = new TimeSeriesExecutionContext(plan.getLanguage(),
        plan.getTimeBuckets(), deadlineMs, Collections.emptyMap(), Collections.emptyMap(), receiversByPlanId);
    BaseTimeSeriesOperator brokerOperator = _timeSeriesBrokerPlanVisitor.compile(brokerFragment,
        brokerExecutionContext, plan.getNumInputServersForExchangePlanNode());
    // Create dispatch observer for each query server
    for (TimeSeriesQueryServerInstance serverInstance : plan.getQueryServerInstances()) {
      String serverId = serverInstance.getInstanceId();
      Deadline deadline = Deadline.after(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      Preconditions.checkState(!deadline.isExpired(), "Deadline expired before query could be sent to servers");
      // Send server fragment to every server
      Worker.TimeSeriesQueryRequest request = Worker.TimeSeriesQueryRequest.newBuilder()
          .addAllDispatchPlan(plan.getSerializedServerFragments())
          .putAllMetadata(initializeTimeSeriesMetadataMap(plan, deadlineMs, requestContext, serverId))
          .putMetadata(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestId))
          .build();
      TimeSeriesDispatchObserver
          dispatchObserver = new TimeSeriesDispatchObserver(receiversByPlanId);
      getOrCreateTimeSeriesDispatchClient(serverInstance).submit(request, deadline, dispatchObserver);
    }
    // Execute broker fragment
    return brokerOperator.nextBlock();
  }

  private void populateConsumers(BaseTimeSeriesPlanNode planNode, Map<String, BlockingQueue<Object>> receiverMap) {
    if (planNode instanceof TimeSeriesExchangeNode) {
      receiverMap.put(planNode.getId(), new ArrayBlockingQueue<>(TimeSeriesDispatchObserver.MAX_QUEUE_CAPACITY));
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      populateConsumers(childNode, receiverMap);
    }
  }

  public static class QueryResult {
    private final ResultTable _resultTable;
    private final List<MultiStageQueryStats.StageStats.Closed> _queryStats;
    private final long _brokerReduceTimeMs;

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
    }

    public ResultTable getResultTable() {
      return _resultTable;
    }

    public List<MultiStageQueryStats.StageStats.Closed> getQueryStats() {
      return _queryStats;
    }

    public long getBrokerReduceTimeMs() {
      return _brokerReduceTimeMs;
    }
  }

  private interface SendRequest<E> {
    void send(DispatchClient dispatchClient, Worker.QueryRequest request, QueryServerInstance serverInstance,
        Deadline deadline, Consumer<AsyncResponse<E>> callbackConsumer);
  }
}
