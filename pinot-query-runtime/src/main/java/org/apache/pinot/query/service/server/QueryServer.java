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
package org.apache.pinot.query.service.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.MseMeter;
import org.apache.pinot.common.metrics.MseMetrics;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.access.AuthorizationInterceptor;
import org.apache.pinot.query.access.QueryAccessControlFactory;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.MultiStageStatsTreeEncoder;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.executor.MetricsExecutor;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// QueryServer is theGRPC server that accepts MSE query plan request sent from
/// [org.apache.pinot.query.service.dispatch.QueryDispatcher].
///
/// All endpoints are being called by the GRPC server threads, which must never be blocked.
/// In some situations we need to run blocking code to resolve these requests, so we need to move the work to a
/// different thread pool. See each method for details.
public class QueryServer extends PinotQueryWorkerGrpc.PinotQueryWorkerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryServer.class);

  // TODO: Inbound messages can get quite large because we send the entire stage metadata map in each call.
  // See https://github.com/apache/pinot/issues/10331
  private static final int MAX_INBOUND_MESSAGE_SIZE = 64 * 1024 * 1024;

  private final String _instanceId;
  private final int _port;
  private final QueryRunner _queryRunner;
  @Nullable
  private final TlsConfig _tlsConfig;
  @Nullable
  private final QueryAccessControlFactory _accessControlFactory;
  private final ThreadAccountant _threadAccountant;
  private final int _permitKeepAliveTimeMs;
  private final boolean _permitKeepAliveWithoutCalls;

  // query submission service is only used for plan submission for now.
  // TODO: with complex query submission logic we should allow asynchronous query submission return instead of
  //   directly return from submission response observer.
  private final ExecutorService _submissionExecutorService;
  /// The thread executor service used to run explain plan requests.
  /// In order to calculate explain plans we need to calculate the physical plan for each worker, which is a
  /// non-blocking but heavier CPU operation than the normal query submission.
  /// This is why we use a different thread pool for this operation: We don't want to block the query submission thread
  /// pool.
  /// Given the QPS for explain plans is much lower than the normal query submission, we can use a single thread for
  /// this.
  private final ExecutorService _explainExecutorService;
  /// The thread executor service used to run time series query requests.
  ///
  /// In order to run time series we need to call [QueryRunner#processTimeSeriesQuery] which contrary to the normal MSE
  /// methods, is a blocking method. This is why we need to use a different (and usually cached) thread pool for this.
  private final ExecutorService _timeSeriesExecutorService;

  private Server _server = null;

  @VisibleForTesting
  public QueryServer(int port, QueryRunner queryRunner) {
    this(port, queryRunner, null);
  }

  @VisibleForTesting
  public QueryServer(int port, QueryRunner queryRunner, @Nullable TlsConfig tlsConfig) {
    this(port, queryRunner, tlsConfig, null);
  }

  @VisibleForTesting
  public QueryServer(int port, QueryRunner queryRunner, @Nullable TlsConfig tlsConfig,
      @Nullable QueryAccessControlFactory accessControlFactory) {
    this(new PinotConfiguration(), "serverId", port, queryRunner, tlsConfig, accessControlFactory,
        ThreadAccountantUtils.getNoOpAccountant());
  }

  public QueryServer(PinotConfiguration serverConf, String instanceId, int port, QueryRunner queryRunner,
      @Nullable TlsConfig tlsConfig, ThreadAccountant threadAccountant) {
    this(serverConf, instanceId, port, queryRunner, tlsConfig, null, threadAccountant);
  }

  public QueryServer(PinotConfiguration serverConf, String instanceId, int port, QueryRunner queryRunner,
      @Nullable TlsConfig tlsConfig, @Nullable QueryAccessControlFactory accessControlFactory,
      ThreadAccountant threadAccountant) {
    _instanceId = instanceId;
    _port = port;
    _queryRunner = queryRunner;
    _tlsConfig = tlsConfig;
    if (accessControlFactory == null) {
      _accessControlFactory = QueryAccessControlFactory.fromConfig(serverConf);
    } else {
      _accessControlFactory = accessControlFactory;
    }
    _threadAccountant = threadAccountant;
    _permitKeepAliveTimeMs = serverConf.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PERMIT_KEEP_ALIVE_TIME_MS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_QUERY_SERVER_PERMIT_KEEP_ALIVE_TIME_MS);
    _permitKeepAliveWithoutCalls = serverConf.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_QUERY_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS);

    ExecutorService baseExecutorService =
        ExecutorServiceUtils.create(serverConf, CommonConstants.Server.MULTISTAGE_SUBMISSION_EXEC_CONFIG_PREFIX,
            "query_submission_executor_on_" + _port + "_port",
            CommonConstants.Server.DEFAULT_MULTISTAGE_SUBMISSION_EXEC_TYPE);

    MseMetrics mseMetrics = MseMetrics.get();
    _submissionExecutorService = new MetricsExecutor(baseExecutorService,
        mseMetrics.getMeteredValue(MseMeter.SUBMISSION_STARTED_TASKS),
        mseMetrics.getMeteredValue(MseMeter.SUBMISSION_COMPLETED_TASKS));

    NamedThreadFactory explainThreadFactory = new NamedThreadFactory("query_explain_on_" + _port + "_port");
    _explainExecutorService = Executors.newSingleThreadExecutor(explainThreadFactory);

    _timeSeriesExecutorService =
        ExecutorServiceUtils.create(serverConf, CommonConstants.Server.MULTISTAGE_TIMESERIES_EXEC_CONFIG_PREFIX,
            "query_ts_on_" + _port + "_port", CommonConstants.Server.DEFAULT_TIMESERIES_EXEC_CONFIG_PREFIX);
  }

  public void start() {
    LOGGER.info("Starting QueryServer");
    try {
      if (_server == null) {
        // Always use NettyServerBuilder so we can configure Netty-specific gRPC server options
        // (e.g. permitKeepAliveTime / permitKeepAliveWithoutCalls).
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(_port);
        if (_tlsConfig != null) {
          serverBuilder.sslContext(getOrCreateServerSslContext());
        }
        _server = buildGrpcServer(serverBuilder);
        LOGGER.info(
            "Initialized QueryServer on port: {} with permitKeepAliveTimeMs: {}, permitKeepAliveWithoutCalls: {}",
            _port, _permitKeepAliveTimeMs, _permitKeepAliveWithoutCalls);
      }
      _queryRunner.start();
      _server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  int getPermitKeepAliveTimeMs() {
    return _permitKeepAliveTimeMs;
  }

  @VisibleForTesting
  boolean isPermitKeepAliveWithoutCalls() {
    return _permitKeepAliveWithoutCalls;
  }

  private Server buildGrpcServer(NettyServerBuilder builder) {
    // By using directExecutor, GRPC doesn't need to manage its own thread pool
    builder.directExecutor();
    if (_accessControlFactory != null) {
      builder.intercept(new AuthorizationInterceptor(_accessControlFactory));
    }
    // Configure server-side keep-alive enforcement so that operators tuning down the broker dispatch keep-alive time
    // (or enabling pings-without-calls) do not get their channels torn down with GOAWAY(ENHANCE_YOUR_CALM).
    if (_permitKeepAliveTimeMs > 0) {
      builder.permitKeepAliveTime(_permitKeepAliveTimeMs, TimeUnit.MILLISECONDS);
    }
    builder.permitKeepAliveWithoutCalls(_permitKeepAliveWithoutCalls);
    return builder.addService(this).maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE).build();
  }

  public void shutdown() {
    LOGGER.info("Shutting down QueryServer");
    try {
      _queryRunner.shutDown();
      if (_server != null) {
        _server.shutdown();
        _server.awaitTermination();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    _submissionExecutorService.shutdown();
    _explainExecutorService.shutdown();
    _timeSeriesExecutorService.shutdown();
  }

  private SslContext getOrCreateServerSslContext() {
    SslContext sslContext = ServerContext.getInstance().getServerGrpcSslContext();
    if (sslContext == null) {
      sslContext = GrpcQueryServer.buildGrpcSslContext(_tlsConfig);
      ServerContext.getInstance().setServerGrpcSslContext(sslContext);
    }
    return sslContext;
  }

  /// Submits a query for executions.
  ///
  /// The request is deserialized on the GRPC thread and the rest of the work is done on the submission thread pool.
  /// Given the submission code should be not blocking, we could directly use the GRPC thread pool, but we decide to
  /// use a different thread pool to be able to time out the submission in the rare case it takes too long.
  ///
  // TODO: Study if that is actually needed. We could just use the GRPC thread pool and timeout once the execution
  //   starts in the query runner.
  @Override
  public void submit(Worker.QueryRequest request, StreamObserver<Worker.QueryResponse> responseObserver) {
    // Match the SSE QUERIES counter semantics by counting requests as soon as they reach the handler.
    MseMetrics.get().addMeteredGlobalValue(MseMeter.QUERIES, 1L);
    Map<String, String> reqMetadata;
    try {
      reqMetadata = QueryPlanSerDeUtils.fromProtoProperties(request.getMetadata());
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing request metadata", e);
      String errorMsg = "Caught exception while deserializing request metadata: " + e.getMessage();
      responseObserver.onNext(Worker.QueryResponse.newBuilder()
          .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
          .build());
      responseObserver.onCompleted();
      return;
    }

    long timeoutMs = Long.parseLong(reqMetadata.get(QueryOptionKey.TIMEOUT_MS));
    CompletableFuture.runAsync(() -> submitInternal(request, reqMetadata), _submissionExecutorService)
        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
        .whenComplete((result, error) -> {
          // this will always be called, either on the submission thread that finished the last or in the caller
          // (GRPC) thread in the improbable case all submission tasks finished before the caller thread reaches
          // this line
          if (error != null) { // if there was an error submitting the request, return an error response
            long requestId = Long.parseLong(reqMetadata.get(MetadataKeys.REQUEST_ID));
            LOGGER.error("Caught exception while submitting request: {}", requestId, error);
            String errorMsg = "Caught exception while submitting request: " + error.getMessage();
            responseObserver.onNext(Worker.QueryResponse.newBuilder()
                .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                .build());
            responseObserver.onCompleted();
          } else { // if the request was submitted successfully, return a success response
            responseObserver.onNext(Worker.QueryResponse.newBuilder()
                .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK, "")
                .build());
            responseObserver.onCompleted();
          }
        });
  }

  /// Iterates over all the stage plans in the request and submits each worker to the query runner.
  ///
  /// This method should be called from the submission thread pool.
  /// If any exception is thrown while submitting a worker, all the workers that have been started are cancelled and the
  /// exception is thrown.
  ///
  /// Remember that this method doesn't track the status of the workers their self. In case of error while the worker is
  /// running, the exception will be managed by the worker, which usually send the error downstream to the receiver
  /// mailboxes. Therefore these error won't be reported here.
  private void submitInternal(Worker.QueryRequest request, Map<String, String> reqMetadata) {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseServerRequest(reqMetadata, _instanceId);
    long requestId = executionContext.getRequestId();
    List<CompletableFuture<Void>> startedWorkers = new ArrayList<>();
    List<Worker.StagePlan> protoStagePlans = request.getStagePlanList();
    for (Worker.StagePlan protoStagePlan : protoStagePlans) {
      StagePlan stagePlan = deserializePlan(requestId, protoStagePlan);
      StageMetadata stageMetadata = stagePlan.getStageMetadata();
      for (WorkerMetadata workerMetadata : stageMetadata.getWorkerMetadataList()) {
        try {
          startedWorkers.add(submitWorker(workerMetadata, stagePlan, reqMetadata, executionContext));
        } catch (Exception e) {
          startedWorkers.forEach(worker -> worker.cancel(true));
          throw e;
        }
      }
    }
  }

  /// Submits creates a new worker by submitting the stage plan to the query runner.
  ///
  /// The returned CompletableFuture will be completed when the opchain starts, which may take a while if there are
  /// pipeline breakers.
  /// Remember that the completable future may (and usually will) be completed *before* the opchain is finished.
  ///
  /// If there is an error scheduling creating the worker (ie too many workers are already running), the
  /// CompletableFuture will be completed exceptionally. In that case it is up to the caller to deal with the error
  /// (normally cancelling other already started workers and sending the error through GRPC)
  private CompletableFuture<Void> submitWorker(WorkerMetadata workerMetadata, StagePlan stagePlan,
      Map<String, String> reqMetadata, QueryExecutionContext executionContext) {
    PlanNode rootNode = stagePlan.getRootNode();
    Set<Integer> upstreamStageIds = collectUpstreamStageIds(rootNode);
    Set<Integer> downstreamStageIds = collectDownstreamStageIds(rootNode);
    QueryThreadContext.MseWorkerInfo mseWorkerInfo =
        new QueryThreadContext.MseWorkerInfo(stagePlan.getStageMetadata().getStageId(), workerMetadata.getWorkerId(),
            upstreamStageIds, downstreamStageIds);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, mseWorkerInfo, _threadAccountant)) {
      return _queryRunner.processQuery(workerMetadata, stagePlan, reqMetadata);
    }
  }

  @Override
  public void explain(Worker.QueryRequest request, StreamObserver<Worker.ExplainResponse> responseObserver) {
    Map<String, String> reqMetadata;
    try {
      reqMetadata = QueryPlanSerDeUtils.fromProtoProperties(request.getMetadata());
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing request metadata", e);
      String errorMsg = "Caught exception while deserializing request metadata: " + e.getMessage();
      responseObserver.onNext(Worker.ExplainResponse.newBuilder()
          .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
          .build());
      responseObserver.onCompleted();
      return;
    }

    long timeoutMs = Long.parseLong(reqMetadata.get(QueryOptionKey.TIMEOUT_MS));
    CompletableFuture.runAsync(() -> explainInternal(request, responseObserver, reqMetadata), _explainExecutorService)
        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
        .handle((result, error) -> {
          if (error != null) {
            long requestId = Long.parseLong(reqMetadata.get(MetadataKeys.REQUEST_ID));
            LOGGER.error("Caught exception while submitting request: {}", requestId, error);
            String errorMsg = "Caught exception while submitting request: " + error.getMessage();
            synchronized (responseObserver) {
              responseObserver.onNext(Worker.ExplainResponse.newBuilder()
                  .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                  .build());
              responseObserver.onCompleted();
            }
          } else {
            synchronized (responseObserver) {
              responseObserver.onNext(Worker.ExplainResponse.newBuilder()
                  .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_OK, "")
                  .build());
              responseObserver.onCompleted();
            }
          }
          return null;
        });
  }

  /// Iterates over all the stage plans in the request and explains the query for each worker.
  ///
  /// This method should be called from the submission thread pool.
  /// Each stage plan will be sent as a new response to the response observer.
  /// In case of failure, this method will fail with an exception and the response observer won't be notified.
  ///
  /// Remember that in case of error some stages may have been explained and some not.
  private void explainInternal(Worker.QueryRequest request, StreamObserver<Worker.ExplainResponse> responseObserver,
      Map<String, String> reqMetadata) {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseServerRequest(reqMetadata, _instanceId);
    long requestId = executionContext.getRequestId();
    for (Worker.StagePlan protoStagePlan : request.getStagePlanList()) {
      StagePlan stagePlan = deserializePlan(requestId, protoStagePlan);
      StageMetadata stageMetadata = stagePlan.getStageMetadata();

      Worker.ExplainResponse.Builder builder = Worker.ExplainResponse.newBuilder();
      PlanNode rootNode = stagePlan.getRootNode();
      Set<Integer> upstreamStageIds = collectUpstreamStageIds(rootNode);
      Set<Integer> downstreamStageIds = collectDownstreamStageIds(rootNode);
      List<WorkerMetadata> workerMetadataList = stageMetadata.getWorkerMetadataList();
      for (WorkerMetadata workerMetadata : workerMetadataList) {
        QueryThreadContext.MseWorkerInfo mseWorkerInfo =
            new QueryThreadContext.MseWorkerInfo(stagePlan.getStageMetadata().getStageId(),
                workerMetadata.getWorkerId(), upstreamStageIds, downstreamStageIds);
        StagePlan explainPlan;
        try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, mseWorkerInfo, _threadAccountant)) {
          explainPlan = _queryRunner.explainQuery(workerMetadata, stagePlan, reqMetadata);
        }
        ByteString rootAsBytes = PlanNodeSerializer.process(explainPlan.getRootNode()).toByteString();
        StageMetadata metadata = explainPlan.getStageMetadata();
        List<Worker.WorkerMetadata> protoWorkerMetadataList =
            QueryPlanSerDeUtils.toProtoWorkerMetadataList(metadata.getWorkerMetadataList());
        builder.addStagePlan(Worker.StagePlan.newBuilder()
            .setRootNode(rootAsBytes)
            .setStageMetadata(Worker.StageMetadata.newBuilder()
                .setStageId(metadata.getStageId())
                .addAllWorkerMetadata(protoWorkerMetadataList)
                .setCustomProperty(QueryPlanSerDeUtils.toProtoProperties(metadata.getCustomProperties()))));
      }
      builder.putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_OK, "");
      synchronized (responseObserver) {
        responseObserver.onNext(builder.build());
      }
    }
  }

  @Override
  public void submitTimeSeries(Worker.TimeSeriesQueryRequest request,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    CompletableFuture.runAsync(() -> submitTimeSeriesInternal(request, responseObserver), _timeSeriesExecutorService);
  }

  private void submitTimeSeriesInternal(Worker.TimeSeriesQueryRequest request,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    Map<String, String> metadataMap = request.getMetadataMap();
    QueryExecutionContext executionContext = QueryExecutionContext.forTseServerRequest(metadataMap, _instanceId);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, _threadAccountant)) {
      _queryRunner.processTimeSeriesQuery(request.getDispatchPlanList(), metadataMap, responseObserver);
    }
  }

  /// Stream-mode submission handler. The broker keeps the stream open for the query lifetime; the server replies
  /// with a {@code submit_ack} as the first message and (in subsequent commits) per-opchain
  /// {@link Worker.OpChainComplete} messages followed by a final {@link Worker.ServerDone}.
  ///
  /// This skeleton wires up the gRPC mechanics + plan submission via the existing submission path. It does NOT yet
  /// emit OpChainComplete / ServerDone — those need a per-opchain completion hook on
  /// {@link org.apache.pinot.query.runtime.executor.OpChainSchedulerService}, which is layered on next.
  /// Cancel still routes through the existing unary {@link #cancel(Worker.CancelRequest, StreamObserver)} RPC; broker
  /// stream-close also triggers a cancel here.
  @Override
  public StreamObserver<Worker.BrokerToServer> submitWithStream(
      StreamObserver<Worker.ServerToBroker> responseObserver) {
    return new SubmitWithStreamObserver(responseObserver);
  }

  /// Per-query state for an open {@code SubmitWithStream} call. Owns the response stream and serialises every
  /// {@code onNext} call on it via a {@code synchronized} block — gRPC requires {@code StreamObserver.onNext} to be
  /// called serially.
  ///
  /// Tracks the expected number of opchains for the request (sum of WorkerMetadata across all stages). An
  /// {@link OpChainCompletionListener} registered with {@link QueryRunner#registerOpChainCompletionListener}
  /// fires once per opchain finishing, encodes its stats via {@link MultiStageStatsTreeEncoder}, and emits an
  /// {@link Worker.OpChainComplete} on the response stream. When the per-request completed-count reaches the
  /// expected total, {@link Worker.ServerDone} is emitted and the stream is closed.
  ///
  /// All blocking work (plan deserialization, opchain construction) runs on
  /// {@link QueryServer#_submissionExecutorService}.
  private final class SubmitWithStreamObserver implements StreamObserver<Worker.BrokerToServer> {
    private final StreamObserver<Worker.ServerToBroker> _responseObserver;
    /// Serialises onNext calls on the response stream and guards mutable session state.
    private final Object _streamLock = new Object();
    /// True once we've received the first {@code submit} and dispatched it.
    private final AtomicBoolean _submitted = new AtomicBoolean(false);
    /// True once we've completed the response stream (success or error). Idempotent guard.
    private final AtomicBoolean _completed = new AtomicBoolean(false);
    /// Number of opchains we expect to report for this request — set after we deserialize the plan.
    private final AtomicInteger _expectedOpChains = new AtomicInteger(-1);
    /// Number of opchains that have reported so far via the completion listener.
    private final AtomicInteger _completedOpChains = new AtomicInteger(0);
    /// Pipeline-breaker root operators awaiting their stage's main (leaf) opchain, keyed by {@link OpChainId}. A
    /// pipeline-breaker opchain shares the same OpChainId as its leaf opchain and is not reported as a separate
    /// stage; its operators are folded into the leaf's flat stats but are absent from the leaf's live operator tree,
    /// so we stash the PB root here and graft it onto the leaf when the leaf opchain reports (see
    /// {@link #onOpChainComplete} and {@link MultiStageStatsTreeEncoder}). Keyed by OpChainId to support multiple
    /// PB-bearing leaf opchains per request (multiple leaf stages / workers). The PB opchain completes strictly
    /// before its leaf — the leaf consumes the PB results, gated on the PB's CountDownLatch in
    /// {@code PipelineBreakerExecutor}, which establishes a happens-before from this {@code put} to the leaf's
    /// {@code remove}; {@link ConcurrentHashMap} also makes the cross-executor-thread read safe directly.
    private final Map<OpChainId, MultiStageOperator> _pipelineBreakerRoots = new ConcurrentHashMap<>();
    /// Set once we successfully parse the request id from the submit metadata. Used by cancel-via-stream.
    private volatile long _requestId = -1;
    /// Completed once {@link #sendSubmitAck} has been called (success or error path). Guards the ack/done race:
    /// if a trivial opchain finishes before the {@code whenComplete} callback fires, {@code onOpChainComplete}
    /// waits for this future via {@code thenRun} instead of calling {@link #sendDoneAndComplete} immediately,
    /// ensuring the broker always receives the {@code submit_ack} before {@code ServerDone}.
    private final CompletableFuture<Void> _ackSentFuture = new CompletableFuture<>();

    SubmitWithStreamObserver(StreamObserver<Worker.ServerToBroker> responseObserver) {
      _responseObserver = responseObserver;
    }

    @Override
    public void onNext(Worker.BrokerToServer message) {
      switch (message.getPayloadCase()) {
        case SUBMIT:
          handleSubmit(message.getSubmit());
          break;
        case CANCEL:
          handleCancel(message.getCancel());
          break;
        case PAYLOAD_NOT_SET:
        default:
          sendErrorAndComplete("Unexpected BrokerToServer payload: " + message.getPayloadCase());
          break;
      }
    }

    @Override
    public void onError(Throwable t) {
      // Broker-side stream error / disconnect. Treat like a cancel and clean up; do not reply on the response stream
      // (the underlying transport is gone). Use compareAndSet to be consistent with sendDoneAndComplete and avoid
      // double-cancelling if onOpChainComplete already completed the stream first.
      LOGGER.warn("SubmitWithStream stream error for request {}: {}", _requestId, t.getMessage());
      if (_completed.compareAndSet(false, true)) {
        cleanupListener();
        cancelIfSubmitted();
      }
    }

    @Override
    public void onCompleted() {
      // Broker has half-closed (no more inbound messages). The server stream stays open until all opchains have
      // reported via the completion listener — it's the listener's job to emit ServerDone and complete the stream.
      // If the broker half-closes before the server is done, that's OK; we keep emitting on the response stream
      // until our own completion criterion is met.
    }

    private void handleSubmit(Worker.QueryRequest request) {
      if (!_submitted.compareAndSet(false, true)) {
        sendErrorAndComplete("Multiple submit messages on the same stream are not allowed");
        return;
      }
      // Count stream-path (SubmitWithStream) queries the same way the unary submit() path does, via the MseMetrics
      // abstraction master introduced (MseMeter.QUERIES forwards to ServerMeter.MSE_QUERIES in SERVER mode).
      MseMetrics.get().addMeteredGlobalValue(MseMeter.QUERIES, 1L);
      Map<String, String> deserializedMetadata;
      try {
        deserializedMetadata = QueryPlanSerDeUtils.fromProtoProperties(request.getMetadata());
      } catch (Exception e) {
        LOGGER.error("Caught exception while deserializing request metadata", e);
        sendErrorAndComplete("Caught exception while deserializing request metadata: " + e.getMessage());
        return;
      }
      // Override the cluster-level _sendStats decision for this request: stats travel out-of-band on the bidi stream
      // (via the OpChainCompletionListener), so we suppress the mailbox-side stats path. The override is read by
      // QueryRunner.effectiveSendStats(...).
      Map<String, String> reqMetadata = new HashMap<>(deserializedMetadata);
      reqMetadata.put(CommonConstants.MultiStageQueryRunner.KEY_OF_STATS_REPORTING_MODE,
          CommonConstants.MultiStageQueryRunner.STATS_REPORTING_MODE_STREAM);
      try {
        _requestId = Long.parseLong(reqMetadata.get(MetadataKeys.REQUEST_ID));
      } catch (Exception ignored) {
        // _requestId stays at -1; cancel-on-stream-close will just be a no-op.
      }
      // Count how many opchains will run on this server: sum of WorkerMetadata across all stage plans.
      int opChainCount = 0;
      for (Worker.StagePlan stagePlan : request.getStagePlanList()) {
        opChainCount += stagePlan.getStageMetadata().getWorkerMetadataCount();
      }
      final int expected = opChainCount;
      // Must set _expectedOpChains BEFORE registerOpChainCompletionListener. The AtomicInteger.set is a volatile
      // write; ConcurrentHashMap.put (inside registerOpChainCompletionListener) provides a subsequent happens-before
      // edge, so any thread that reads via ConcurrentHashMap.get (the listener callback) is guaranteed to observe
      // the _expectedOpChains value set here. Reordering these two lines would break the JMM guarantee.
      _expectedOpChains.set(expected);

      // Register the per-request completion listener BEFORE submitting. Otherwise short opchains could finish before
      // we've registered and we'd miss their events.
      if (_requestId >= 0 && expected > 0) {
        _queryRunner.registerOpChainCompletionListener(_requestId, this::onOpChainComplete);
      }

      long timeoutMs = Long.parseLong(reqMetadata.get(QueryOptionKey.TIMEOUT_MS));
      CompletableFuture.runAsync(() -> submitInternal(request, reqMetadata), _submissionExecutorService)
          .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
          .whenComplete((result, error) -> {
            if (error != null) {
              LOGGER.error("Caught exception while submitting request: {}", _requestId, error);
              sendSubmitAck(buildErrorResponse("Caught exception while submitting request: " + error.getMessage()));
              // Submission failed — no opchains will run, so emit ServerDone immediately and clean up.
              cleanupListener();
              sendDoneAndComplete();
            } else {
              sendSubmitAck(buildOkResponse());
              // If for some reason expected was 0 (empty plan), close the stream now.
              if (expected == 0) {
                cleanupListener();
                sendDoneAndComplete();
              }
            }
            // Signal that the ack has been sent (regardless of success/error). Any onOpChainComplete invocation
            // that raced ahead of this whenComplete callback will be unblocked via thenRun.
            _ackSentFuture.complete(null);
          });
    }

    /**
     * Fires once per opchain on this server completing. Encodes the stats into a {@link Worker.MultiStageStatsTree},
     * emits an {@link Worker.OpChainComplete}, and emits {@link Worker.ServerDone} once all expected opchains have
     * reported.
     */
    private void onOpChainComplete(OpChainId opChainId, MultiStageOperator rootOperator,
        @Nullable MultiStageQueryStats stats, OpChainExecutionContext context, @Nullable Throwable error) {
      // A pipeline-breaker opchain (dynamic-broadcast semi-join / lookup-join build side) is built from the SAME
      // OpChainExecutionContext as its stage's main (leaf) opchain, so it carries an identical OpChainId and fires
      // this listener too. It must NOT be reported as a separate stage:
      //   - _expectedOpChains counts only the main per-worker opchains, so counting the PB here would make
      //     _completedOpChains reach the expected total one early, prematurely firing ServerDone and causing the
      //     real opchain's OpChainComplete to be dropped by the !_completed.get() guard at the send site.
      //   - its stats are not lost: the PB's operators are already folded into the leaf opchain's flat stats (via
      //     LeafOperator.calculateUpstreamStats, exactly as in the legacy mailbox path), but they are absent from
      //     the leaf's live operator tree, which is what the encoder walks. So we stash the PB root here and graft
      //     it onto the leaf below, letting the stage report once — when its leaf opchain completes — with one
      //     coherent tree.
      if (rootOperator.getOperatorType() == MultiStageOperator.Type.PIPELINE_BREAKER) {
        _pipelineBreakerRoots.put(opChainId, rootOperator);
        return;
      }
      Worker.OpChainComplete.Builder builder = Worker.OpChainComplete.newBuilder()
          .setStageId(opChainId.getStageId())
          .setWorkerId(opChainId.getVirtualServerId())
          .setSuccess(error == null);
      if (error != null) {
        builder.setErrorMsg(error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
      }
      // Claim any stashed pipeline-breaker root for this stage (remove() so the stash self-cleans, even on the
      // leaf-error path below where it is unused). The PB always completed before this leaf opchain (see
      // _pipelineBreakerRoots), so it is already present when the stage folded one.
      MultiStageOperator pipelineBreakerRoot = _pipelineBreakerRoots.remove(opChainId);
      if (stats != null) {
        // If this stage folded a pipeline breaker, graft its stashed operator subtree onto the leaf so the encoder's
        // tree walk realigns with the leaf opchain's folded flat stats.
        try {
          builder.setStats(MultiStageStatsTreeEncoder.encode(rootOperator, stats, context, pipelineBreakerRoot));
        } catch (Throwable t) {
          // Encoding failed — no partial stats can be recovered. The encoder is all-or-nothing by design:
          //   1. The upfront treeSize != flatSize check throws IllegalStateException before any proto node is built.
          //   2. An IOException from serializeStatMap mid-walk leaves partial StageStatsNode builders only on the
          //      Java call stack; they are discarded when the exception unwinds. No partially-built
          //      MultiStageStatsTree is ever returned.
          // We deliberately do NOT set success=false here. The opchain computation itself succeeded (error==null
          // at the top of this method), so we preserve success=true and send empty stats instead. Setting
          // success=false would cause the broker to treat the opchain as a peer error and fire fanOutCancel,
          // cancelling a completely healthy query just because stats serialization failed.
          LOGGER.warn("Failed to encode stats tree for opchain {}", opChainId, t);
          builder.setErrorMsg(builder.getErrorMsg().isEmpty() ? "stats encode failed: " + t.getMessage()
              : builder.getErrorMsg() + "; stats encode failed: " + t.getMessage());
        }
      }
      Worker.ServerToBroker message = Worker.ServerToBroker.newBuilder().setOpchain(builder).build();
      try {
        synchronized (_streamLock) {
          if (!_completed.get()) {
            _responseObserver.onNext(message);
          }
        }
      } finally {
        // Increment in finally: if onNext throws a transport exception (e.g. the broker closed the stream early),
        // we must still count this opchain as done so that sendDoneAndComplete fires when the last one finishes.
        // Without this, a single dropped send leaves _completedOpChains short of _expectedOpChains permanently,
        // and the broker waits on its drain latch until the timeout instead of receiving ServerDone promptly.
        if (_completedOpChains.incrementAndGet() >= _expectedOpChains.get()) {
          // Wait for the ack to be sent before emitting ServerDone. If whenComplete already ran, thenRun fires
          // immediately on this thread; if not (trivial-plan race), it fires on the whenComplete thread once
          // the ack is dispatched. sendDoneAndComplete is guarded by compareAndSet so double-invocation is safe.
          _ackSentFuture.thenRun(() -> {
            cleanupListener();
            sendDoneAndComplete();
          });
        }
      }
    }

    private void cleanupListener() {
      if (_requestId >= 0) {
        _queryRunner.unregisterOpChainCompletionListener(_requestId);
      }
    }

    private void handleCancel(Worker.CancelRequest cancel) {
      // The broker still uses the unary Cancel RPC in Phase A; cancel-via-stream is plumbed through here so that
      // future Phase B brokers can use the same channel without server-side changes.
      try {
        _queryRunner.cancel(cancel.getRequestId());
      } catch (Throwable t) {
        LOGGER.warn("Caught exception cancelling request {} via SubmitWithStream", cancel.getRequestId(), t);
      }
    }

    private void cancelIfSubmitted() {
      if (_submitted.get() && _requestId >= 0) {
        try {
          _queryRunner.cancel(_requestId);
        } catch (Throwable t) {
          LOGGER.warn("Caught exception cancelling request {} after stream error", _requestId, t);
        }
      }
    }

    private void sendSubmitAck(Worker.QueryResponse ack) {
      synchronized (_streamLock) {
        if (_completed.get()) {
          return;
        }
        _responseObserver.onNext(Worker.ServerToBroker.newBuilder().setSubmitAck(ack).build());
      }
    }

    private void sendDoneAndComplete() {
      synchronized (_streamLock) {
        if (_completed.compareAndSet(false, true)) {
          try {
            _responseObserver.onNext(Worker.ServerToBroker.newBuilder()
                .setDone(Worker.ServerDone.getDefaultInstance())
                .build());
          } catch (Throwable t) {
            LOGGER.warn("Failed to send ServerDone for request {}", _requestId, t);
          }
          // Always attempt onCompleted even if the ServerDone send failed, so the gRPC stream is half-closed
          // on our side and the broker can detect the clean stream end via its onCompleted callback.
          try {
            _responseObserver.onCompleted();
          } catch (Throwable t) {
            LOGGER.warn("Failed to complete response stream for request {}", _requestId, t);
          }
        }
      }
    }

    private void sendErrorAndComplete(String errorMsg) {
      synchronized (_streamLock) {
        if (_completed.compareAndSet(false, true)) {
          _responseObserver.onNext(Worker.ServerToBroker.newBuilder()
              .setSubmitAck(buildErrorResponse(errorMsg))
              .build());
          _responseObserver.onCompleted();
        }
      }
    }

    private Worker.QueryResponse buildOkResponse() {
      return Worker.QueryResponse.newBuilder()
          .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK, "")
          .build();
    }

    private Worker.QueryResponse buildErrorResponse(String errorMsg) {
      return Worker.QueryResponse.newBuilder()
          .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
          .build();
    }
  }

  /// Executes a cancel request.
  ///
  /// Cancel requests should always be executed on different threads than the submission threads to be sure that
  /// the cancel request is not waiting for the query submission to finish. Given these requests are not blocking, we
  /// run them in the GRPC thread pool.
  @Override
  public void cancel(Worker.CancelRequest request, StreamObserver<Worker.CancelResponse> responseObserver) {
    long requestId = request.getRequestId();
    try {
      _queryRunner.cancel(requestId);
    } catch (Throwable t) {
      LOGGER.error("Caught exception while cancelling opChain for request: {}", requestId, t);
    }
    // we always return completed even if cancel attempt fails, server will self clean up in this case.
    responseObserver.onNext(Worker.CancelResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  // Collects sender stage IDs from all MailboxReceiveNodes in the plan tree via BFS,
  // since MailboxReceiveNodes can appear at any depth (e.g. joins have multiple inputs).
  @VisibleForTesting
  static Set<Integer> collectUpstreamStageIds(PlanNode rootNode) {
    Set<Integer> ids = new HashSet<>();
    ArrayDeque<PlanNode> queue = new ArrayDeque<>();
    queue.add(rootNode);
    while (!queue.isEmpty()) {
      PlanNode node = queue.poll();
      if (node instanceof MailboxReceiveNode) {
        ids.add(((MailboxReceiveNode) node).getSenderStageId());
      }
      queue.addAll(node.getInputs());
    }
    return ids;
  }

  // Collects receiver stage IDs from the root MailboxSendNode.
  @VisibleForTesting
  static Set<Integer> collectDownstreamStageIds(PlanNode rootNode) {
    if (rootNode instanceof MailboxSendNode) {
      Set<Integer> ids = new HashSet<>();
      for (int receiverId : ((MailboxSendNode) rootNode).getReceiverStageIds()) {
        ids.add(receiverId);
      }
      return ids;
    }
    return Set.of();
  }

  private StagePlan deserializePlan(long requestId, Worker.StagePlan protoStagePlan) {
    try {
      return QueryPlanSerDeUtils.fromProtoStagePlan(protoStagePlan);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while deserializing stage plan for request: %d, stage: %d", requestId,
              protoStagePlan.getStageMetadata().getStageId()), e);
    }
  }
}
