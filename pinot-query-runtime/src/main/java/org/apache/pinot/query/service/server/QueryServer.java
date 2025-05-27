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
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.MseWorkerThreadContext;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.executor.MetricsExecutor;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
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

  private final int _port;
  private final QueryRunner _queryRunner;
  @Nullable
  private final TlsConfig _tlsConfig;
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
  public QueryServer(int port, QueryRunner queryRunner, @Nullable TlsConfig tlsConfig) {
    this(port, queryRunner, tlsConfig, new PinotConfiguration());
  }

  public QueryServer(int port, QueryRunner queryRunner, @Nullable TlsConfig tlsConfig, PinotConfiguration config) {
    _port = port;
    _queryRunner = queryRunner;
    _tlsConfig = tlsConfig;

    ExecutorService baseExecutorService = ExecutorServiceUtils.create(config,
        CommonConstants.Server.MULTISTAGE_SUBMISSION_EXEC_CONFIG_PREFIX,
        "query_submission_executor_on_" + _port + "_port",
        CommonConstants.Server.DEFAULT_MULTISTAGE_SUBMISSION_EXEC_TYPE);

    ServerMetrics serverMetrics = ServerMetrics.get();
    _submissionExecutorService = new MetricsExecutor(
        baseExecutorService,
        serverMetrics.getMeteredValue(ServerMeter.MULTI_STAGE_SUBMISSION_STARTED_TASKS),
        serverMetrics.getMeteredValue(ServerMeter.MULTI_STAGE_SUBMISSION_COMPLETED_TASKS));

    NamedThreadFactory explainThreadFactory =
        new NamedThreadFactory("query_explain_on_" + _port + "_port");
    _explainExecutorService = Executors.newSingleThreadExecutor(explainThreadFactory);

    ExecutorService baseTimeSeriesExecutorService = ExecutorServiceUtils.create(config,
        CommonConstants.Server.MULTISTAGE_TIMESERIES_EXEC_CONFIG_PREFIX,
        "query_ts_on_" + _port + "_port",
        CommonConstants.Server.DEFAULT_TIMESERIES_EXEC_CONFIG_PREFIX);
    _timeSeriesExecutorService = MseWorkerThreadContext.contextAwareExecutorService(
        QueryThreadContext.contextAwareExecutorService(baseTimeSeriesExecutorService));
  }

  public void start() {
    LOGGER.info("Starting QueryServer");
    try {
      if (_server == null) {
        ServerBuilder<?> serverBuilder;
        if (_tlsConfig == null) {
          serverBuilder = ServerBuilder.forPort(_port);
        } else {
          serverBuilder = NettyServerBuilder.forPort(_port)
              .sslContext(GrpcQueryServer.buildGrpcSslContext(_tlsConfig));
        }
        _server = buildGrpcServer(serverBuilder);
        LOGGER.info("Initialized QueryServer on port: {}", _port);
      }
      _queryRunner.start();
      _server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <T extends ServerBuilder<T>> Server buildGrpcServer(ServerBuilder<T> builder) {
    return builder
         // By using directExecutor, GRPC doesn't need to manage its own thread pool
        .directExecutor()
        .addService(this)
        .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
        .build();
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

    long timeoutMs = Long.parseLong(reqMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));

    CompletableFuture.runAsync(() -> submitInternal(request, reqMetadata), _submissionExecutorService)
        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
        .whenComplete((result, error) -> {
          // this will always be called, either on the submission thread that finished the last or in the caller
          // (GRPC) thread in the improbable case all submission tasks finished before the caller thread reaches
          // this line
          if (error != null) { // if there was an error submitting the request, return an error response
            try (QueryThreadContext.CloseableContext qCtx = QueryThreadContext.openFromRequestMetadata(reqMetadata);
                QueryThreadContext.CloseableContext mseCtx = MseWorkerThreadContext.open()) {
              long requestId = QueryThreadContext.getRequestId();
              LOGGER.error("Caught exception while submitting request: {}", requestId, error);
              String errorMsg = "Caught exception while submitting request: " + error.getMessage();
              responseObserver.onNext(Worker.QueryResponse.newBuilder()
                  .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                  .build());
              responseObserver.onCompleted();
            }
          } else { // if the request was submitted successfully, return a success response
            responseObserver.onNext(
                Worker.QueryResponse.newBuilder()
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
    try (QueryThreadContext.CloseableContext qTlClosable = QueryThreadContext.openFromRequestMetadata(reqMetadata);
        QueryThreadContext.CloseableContext mseTlCloseable = MseWorkerThreadContext.open()) {
      QueryThreadContext.setQueryEngine("mse");
      List<Worker.StagePlan> protoStagePlans = request.getStagePlanList();

      List<CompletableFuture<Void>> startedWorkers = new ArrayList<>();

      for (Worker.StagePlan protoStagePlan : protoStagePlans) {
        StagePlan stagePlan = deserializePlan(protoStagePlan);
        StageMetadata stageMetadata = stagePlan.getStageMetadata();
        List<WorkerMetadata> workerMetadataList = stageMetadata.getWorkerMetadataList();

        for (WorkerMetadata workerMetadata : workerMetadataList) {
          try {
            CompletableFuture<Void> job = submitWorker(workerMetadata, stagePlan, reqMetadata);
            startedWorkers.add(job);
          } catch (RuntimeException e) {
            startedWorkers.forEach(worker -> worker.cancel(true));
            throw e;
          }
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
      Map<String, String> reqMetadata) {
    String requestIdStr = Long.toString(QueryThreadContext.getRequestId());

    //TODO: Verify if this matches with what OOM protection expects. This method will not block for the query to
    // finish, so it may be breaking some of the OOM protection assumptions.
    Tracing.ThreadAccountantOps.setupRunner(requestIdStr, ThreadExecutionContext.TaskType.MSE);
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();

    try {
      return _queryRunner.processQuery(workerMetadata, stagePlan, reqMetadata, parentContext);
    } finally {
      Tracing.ThreadAccountantOps.clear();
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

    long timeoutMs = Long.parseLong(reqMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));

    CompletableFuture.runAsync(() -> explainInternal(request, responseObserver, reqMetadata), _explainExecutorService)
        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
        .handle((result, error) -> {
      if (error != null) {
        try (QueryThreadContext.CloseableContext qCtx = QueryThreadContext.openFromRequestMetadata(reqMetadata);
            QueryThreadContext.CloseableContext mseCtx = MseWorkerThreadContext.open()) {
          long requestId = QueryThreadContext.getRequestId();
          LOGGER.error("Caught exception while submitting request: {}", requestId, error);
          String errorMsg = "Caught exception while submitting request: " + error.getMessage();
          synchronized (responseObserver) {
            responseObserver.onNext(Worker.ExplainResponse.newBuilder()
                .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                .build());
            responseObserver.onCompleted();
          }
        }
      } else {
        synchronized (responseObserver) {
          responseObserver.onNext(
              Worker.ExplainResponse.newBuilder()
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
    try (QueryThreadContext.CloseableContext qTlClosable = QueryThreadContext.openFromRequestMetadata(reqMetadata);
        QueryThreadContext.CloseableContext mseTlCloseable = MseWorkerThreadContext.open()) {
      // Explain the stage for each worker
      BiFunction<StagePlan, WorkerMetadata, StagePlan> explainFun = (stagePlan, workerMetadata) ->
          _queryRunner.explainQuery(workerMetadata, stagePlan, reqMetadata);

      List<Worker.StagePlan> protoStagePlans = request.getStagePlanList();

      for (Worker.StagePlan protoStagePlan : protoStagePlans) {
        StagePlan stagePlan = deserializePlan(protoStagePlan);
        StageMetadata stageMetadata = stagePlan.getStageMetadata();
        List<WorkerMetadata> workerMetadataList = stageMetadata.getWorkerMetadataList();

        Worker.ExplainResponse stageResponse = calculateExplainPlanForStage(
            protoStagePlan, workerMetadataList.toArray(new WorkerMetadata[0]), reqMetadata);
        synchronized (responseObserver) {
          responseObserver.onNext(stageResponse);
        }
      }
    }
  }

  /// Calculates the explain plan for a stage, iterating over each worker and merging the results.
  ///
  /// The result is then returned as result.
  private Worker.ExplainResponse calculateExplainPlanForStage(Worker.StagePlan protoStagePlan,
      WorkerMetadata[] workerMetadataList, Map<String, String> reqMetadata) {
    Worker.ExplainResponse.Builder builder = Worker.ExplainResponse.newBuilder();
    StagePlan stagePlan = deserializePlan(protoStagePlan);
    for (WorkerMetadata workerMetadata : workerMetadataList) {
      StagePlan explainPlan = _queryRunner.explainQuery(workerMetadata, stagePlan, reqMetadata);

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
    return builder.build();
  }

  @Override
  public void submitTimeSeries(Worker.TimeSeriesQueryRequest request,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    try (QueryThreadContext.CloseableContext qCtx = QueryThreadContext.open();
        QueryThreadContext.CloseableContext mseCtx = MseWorkerThreadContext.open()) {
      // TODO: populate the thread context with TSE information
      QueryThreadContext.setQueryEngine("tse");

      CompletableFuture.runAsync(() -> submitTimeSeriesInternal(request, responseObserver), _timeSeriesExecutorService);
    }
  }

  private void submitTimeSeriesInternal(Worker.TimeSeriesQueryRequest request,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    _queryRunner.processTimeSeriesQuery(request.getDispatchPlanList(), request.getMetadataMap(), responseObserver);
  }

  /// Executes a cancel request.
  ///
  /// Cancel requests should always be executed on different threads than the submission threads to be sure that
  /// the cancel request is not waiting for the query submission to finish. Given these requests are not blocking, we
  /// run them in the GRPC thread pool.
  @Override
  public void cancel(Worker.CancelRequest request, StreamObserver<Worker.CancelResponse> responseObserver) {
    long requestId = request.getRequestId();
    try (QueryThreadContext.CloseableContext closeable = QueryThreadContext.open()) {
      QueryThreadContext.setIds(requestId, request.getCid().isBlank() ? request.getCid() : Long.toString(requestId));
      Map<Integer, MultiStageQueryStats.StageStats.Closed> stats = _queryRunner.cancel(requestId);

      Worker.CancelResponse.Builder cancelBuilder = Worker.CancelResponse.newBuilder();
      for (Map.Entry<Integer, MultiStageQueryStats.StageStats.Closed> statEntry : stats.entrySet()) {
        // even we are using output streams here, these calls are non-blocking because we use in memory output streams
        try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream.Builder().get();
            DataOutputStream daos = new DataOutputStream(baos)) {
          statEntry.getValue().serialize(daos);

          daos.flush();
          byte[] byteArray = baos.toByteArray();
          ByteString bytes = UnsafeByteOperations.unsafeWrap(byteArray);
          cancelBuilder.putStatsByStage(statEntry.getKey(), bytes);
        }
      }
      responseObserver.onNext(cancelBuilder.build());
    } catch (Throwable t) {
      LOGGER.error("Caught exception while cancelling opChain for request: {}", requestId, t);
    }
    // we always return completed even if cancel attempt fails, server will self clean up in this case.
    responseObserver.onCompleted();
  }

  private StagePlan deserializePlan(Worker.StagePlan protoStagePlan) {
    try {
      return QueryPlanSerDeUtils.fromProtoStagePlan(protoStagePlan);
    } catch (Exception e) {
      long requestId = QueryThreadContext.getRequestId();
      throw new RuntimeException(
          String.format("Caught exception while deserializing stage plan for request: %d, stage: %d", requestId,
              protoStagePlan.getStageMetadata().getStageId()), e);
    }
  }
}
