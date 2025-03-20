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

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.TlsConfig;
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
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryServer} is the GRPC server that accepts query plan requests sent from {@link QueryDispatcher}.
 */
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
  private final ExecutorService _querySubmissionExecutorService;

  private Server _server = null;

  public QueryServer(int port, QueryRunner queryRunner, @Nullable TlsConfig tlsConfig) {
    _port = port;
    _queryRunner = queryRunner;
    _tlsConfig = tlsConfig;
    _querySubmissionExecutorService = MseWorkerThreadContext.contextAwareExecutorService(
        QueryThreadContext.contextAwareExecutorService(
            Executors.newCachedThreadPool(
                new NamedThreadFactory("query_submission_executor_on_" + _port + "_port")
            )
        )
    );
  }

  public void start() {
    LOGGER.info("Starting QueryServer");
    try {
      if (_server == null) {
        if (_tlsConfig == null) {
          _server = ServerBuilder
              .forPort(_port)
              .addService(this)
              .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
              .build();
        } else {
          _server = NettyServerBuilder
              .forPort(_port)
              .addService(this)
              .sslContext(GrpcQueryServer.buildGrpcSslContext(_tlsConfig))
              .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
              .build();
        }
        LOGGER.info("Initialized QueryServer on port: {}", _port);
      }
      _queryRunner.start();
      _server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down QueryServer");
    try {
      _queryRunner.shutDown();
      if (_server != null) {
        _server.shutdown();
        _server.awaitTermination();
      }
      _querySubmissionExecutorService.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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

    try (QueryThreadContext.CloseableContext queryTlClosable = QueryThreadContext.openFromRequestMetadata(reqMetadata);
        QueryThreadContext.CloseableContext mseTlCloseable = MseWorkerThreadContext.open()) {
      long requestId = QueryThreadContext.getRequestId();
      QueryThreadContext.setQueryEngine("mse");

      Tracing.ThreadAccountantOps.setupRunner(Long.toString(requestId), ThreadExecutionContext.TaskType.MSE);
      ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
      try {
        forEachStage(request,
            (stagePlan, workerMetadata) -> {
              _queryRunner.processQuery(workerMetadata, stagePlan, reqMetadata, parentContext);
              return null;
            },
            (ignored) -> {
            });
      } catch (ExecutionException | InterruptedException | TimeoutException | RuntimeException e) {
        LOGGER.error("Caught exception while submitting request: {}", requestId, e);
        String errorMsg = "Caught exception while submitting request: " + e.getMessage();
        responseObserver.onNext(Worker.QueryResponse.newBuilder()
            .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                .build());
        responseObserver.onCompleted();
        return;
      } finally {
        Tracing.ThreadAccountantOps.clear();
      }
      responseObserver.onNext(
          Worker.QueryResponse.newBuilder()
              .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK, "")
              .build());
      responseObserver.onCompleted();
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

    try (QueryThreadContext.CloseableContext queryTlClosable = QueryThreadContext.openFromRequestMetadata(reqMetadata);
        QueryThreadContext.CloseableContext mseTlCloseable = MseWorkerThreadContext.open()) {
      try {
        forEachStage(request,
            (stagePlan, workerMetadata) -> _queryRunner.explainQuery(workerMetadata, stagePlan, reqMetadata),
            (plans) -> {
              Worker.ExplainResponse.Builder builder = Worker.ExplainResponse.newBuilder();
              for (StagePlan plan : plans) {
                ByteString rootAsBytes = PlanNodeSerializer.process(plan.getRootNode()).toByteString();

                StageMetadata metadata = plan.getStageMetadata();
                List<Worker.WorkerMetadata> protoWorkerMetadataList =
                    QueryPlanSerDeUtils.toProtoWorkerMetadataList(metadata.getWorkerMetadataList());

                builder.addStagePlan(Worker.StagePlan.newBuilder().setRootNode(rootAsBytes).setStageMetadata(
                    Worker.StageMetadata.newBuilder().setStageId(metadata.getStageId())
                        .addAllWorkerMetadata(protoWorkerMetadataList)
                        .setCustomProperty(QueryPlanSerDeUtils.toProtoProperties(metadata.getCustomProperties()))));
              }
              builder.putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_OK, "");
              responseObserver.onNext(builder.build());
            });
      } catch (ExecutionException | InterruptedException | TimeoutException | RuntimeException e) {
        long requestId = QueryThreadContext.getRequestId();
        LOGGER.error("Caught exception while submitting request: {}", requestId, e);
        String errorMsg = "Caught exception while submitting request: " + e.getMessage();
        responseObserver.onNext(Worker.ExplainResponse.newBuilder()
            .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_ERROR, errorMsg)
                .build());
        responseObserver.onCompleted();
        return;
      }
      responseObserver.onNext(
          Worker.ExplainResponse.newBuilder()
              .putMetadata(CommonConstants.Explain.Response.ServerResponseStatus.STATUS_OK, "")
              .build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void submitTimeSeries(Worker.TimeSeriesQueryRequest request,
      StreamObserver<Worker.TimeSeriesResponse> responseObserver) {
    try (QueryThreadContext.CloseableContext closeable = QueryThreadContext.open()) {
      // TODO: populate the thread context with TSE information
      QueryThreadContext.setQueryEngine("tse");
      _queryRunner.processTimeSeriesQuery(request.getDispatchPlanList(), request.getMetadataMap(), responseObserver);
    }
  }

  @Override
  public void cancel(Worker.CancelRequest request, StreamObserver<Worker.CancelResponse> responseObserver) {
    long requestId = request.getRequestId();
    try (QueryThreadContext.CloseableContext closeable = QueryThreadContext.open()) {
      QueryThreadContext.setIds(requestId, request.getCid().isBlank() ? request.getCid() : Long.toString(requestId));
      _queryRunner.cancel(requestId);
    } catch (Throwable t) {
      LOGGER.error("Caught exception while cancelling opChain for request: {}", requestId, t);
    }
    // we always return completed even if cancel attempt fails, server will self clean up in this case.
    responseObserver.onCompleted();
  }

  private <W> void submitStage(
      Worker.StagePlan protoStagePlan,
      BiFunction<StagePlan, WorkerMetadata, W> submitFunction,
      Consumer<W> consumer) {
    StagePlan stagePlan;
    try {
      stagePlan = QueryPlanSerDeUtils.fromProtoStagePlan(protoStagePlan);
    } catch (Exception e) {
      long requestId = QueryThreadContext.getRequestId();
      throw new RuntimeException(
          String.format("Caught exception while deserializing stage plan for request: %d, stage: %d", requestId,
              protoStagePlan.getStageMetadata().getStageId()), e);
    }
    StageMetadata stageMetadata = stagePlan.getStageMetadata();
    MseWorkerThreadContext.setStageId(stageMetadata.getStageId());
    List<WorkerMetadata> workerMetadataList = stageMetadata.getWorkerMetadataList();
    int numWorkers = workerMetadataList.size();
    CompletableFuture<W>[] workerSubmissionStubs = new CompletableFuture[numWorkers];
    for (int j = 0; j < numWorkers; j++) {
      WorkerMetadata workerMetadata = workerMetadataList.get(j);
      workerSubmissionStubs[j] = CompletableFuture.supplyAsync(
          () -> {
            MseWorkerThreadContext.setWorkerId(workerMetadata.getWorkerId());
            return submitFunction.apply(stagePlan, workerMetadata);
          },
          _querySubmissionExecutorService);
    }

    try {
      long deadlineMs = QueryThreadContext.getDeadlineMs();
      for (int j = 0; j < numWorkers; j++) {
        W workerResult = workerSubmissionStubs[j].get(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        consumer.accept(workerResult);
      }
    } catch (TimeoutException e) {
      long requestId = QueryThreadContext.getRequestId();
      throw new RuntimeException(
          "Timeout while submitting request: " + requestId + ", stage: " + stageMetadata.getStageId(), e);
    } catch (Exception e) {
      long requestId = QueryThreadContext.getRequestId();
      throw new RuntimeException(
          "Caught exception while submitting request: " + requestId + ", stage: " + stageMetadata.getStageId()
              + ": " + e.getMessage(), e);
    } finally {
      for (CompletableFuture<?> future : workerSubmissionStubs) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }

  /**
   * Submits each stage in the request to the workers and waits for all workers to complete,
   * applying the submitFunction to each worker and the consumer to the list of results.
   *
   * @param request the query request
   * @param submitFunction the function to apply to each worker.
   * @param consumer the consumer to apply to the list of results. It can just ignore the results if not needed.
   * @param <W> the type of the result returned by the submitFunction.
   */
  <W> void forEachStage(Worker.QueryRequest request,
      BiFunction<StagePlan, WorkerMetadata, W> submitFunction, Consumer<List<W>> consumer)
      throws ExecutionException, InterruptedException, TimeoutException {
    List<Worker.StagePlan> protoStagePlans = request.getStagePlanList();
    int numStages = protoStagePlans.size();
    List<CompletableFuture<List<W>>> stageSubmissionStubs = new ArrayList<>(numStages);
    for (Worker.StagePlan protoStagePlan : protoStagePlans) {
      CompletableFuture<List<W>> future = CompletableFuture.supplyAsync(() -> {
        List<W> plans = new ArrayList<>();
        submitStage(protoStagePlan, submitFunction, plans::add);
        return plans;
      }, _querySubmissionExecutorService);
      stageSubmissionStubs.add(future);
    }
    try {
      long deadlineMs = QueryThreadContext.getDeadlineMs();
      for (CompletableFuture<List<W>> stageSubmissionStub : stageSubmissionStubs) {
        List<W> plans = stageSubmissionStub.get(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        consumer.accept(plans);
      }
    } finally {
      for (CompletableFuture<?> future : stageSubmissionStubs) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }
}
