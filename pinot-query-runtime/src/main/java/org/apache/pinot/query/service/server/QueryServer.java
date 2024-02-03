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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.log4j.builders.appender.SocketAppenderBuilder.LOGGER;


/**
 * {@link QueryServer} is the GRPC server that accepts query plan requests sent from {@link QueryDispatcher}.
 */
public class QueryServer extends PinotQueryWorkerGrpc.PinotQueryWorkerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryServer.class);
  // TODO: Inbound messages can get quite large because we send the entire stage metadata map in each call.
  // See https://github.com/apache/pinot/issues/10331
  private static final int MAX_INBOUND_MESSAGE_SIZE = 64 * 1024 * 1024;

  private final int _port;
  private final PinotConfiguration _configuration;
  private final QueryRunner _queryRunner;
  // query submission service is only used for plan submission for now.
  // TODO: with complex query submission logic we should allow asynchronous query submission return instead of
  //   directly return from submission response observer.
  private final ExecutorService _querySubmissionExecutorService;

  private Server _server = null;

  public QueryServer(int port, QueryRunner queryRunner, PinotConfiguration configuration) {
    _port = port;
    _queryRunner = queryRunner;
    _configuration = configuration;
    _querySubmissionExecutorService =
        Executors.newCachedThreadPool(new NamedThreadFactory("query_submission_executor_on_" + _port + "_port"));
  }

  public void start() {
    LOGGER.info("Starting QueryServer");
    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(_configuration, CommonConstants.Server.SERVER_GRPCTLS_PREFIX);
    if (tlsConfig != null) {
      try {
        _server = NettyServerBuilder.forPort(_port).addService(this).sslContext(buildGRpcSslContext(tlsConfig))
            .maxInboundMessageSize(_configuration.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
                CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES)).build();
        _queryRunner.start();
        _server.start();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start secure grpcMailboxServer", e);
      }
    }else {
      _server = NettyServerBuilder.forPort(_port).addService(this).maxInboundMessageSize(
          _configuration.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
              CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES)).build();
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
    // Deserialize the request
    List<DistributedStagePlan> distributedStagePlans;
    Map<String, String> requestMetadata;
    requestMetadata = Collections.unmodifiableMap(request.getMetadataMap());
    long requestId = Long.parseLong(requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    // 1. Deserialized request
    try {
      distributedStagePlans = QueryPlanSerDeUtils.deserializeStagePlan(request);
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing the request: {}", requestId, e);
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad request").withCause(e).asException());
      return;
    }
    // 2. Submit distributed stage plans, await response successful or any failure which cancels all other tasks.
    int numSubmission = distributedStagePlans.size();
    CompletableFuture<?>[] submissionStubs = new CompletableFuture[numSubmission];
    for (int i = 0; i < numSubmission; i++) {
      DistributedStagePlan distributedStagePlan = distributedStagePlans.get(i);
      submissionStubs[i] =
          CompletableFuture.runAsync(() -> _queryRunner.processQuery(distributedStagePlan, requestMetadata),
              _querySubmissionExecutorService);
    }
    try {
      CompletableFuture.allOf(submissionStubs).get(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOGGER.error("error occurred during stage submission for {}:\n{}", requestId, e);
      responseObserver.onNext(Worker.QueryResponse.newBuilder()
          .putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
              QueryException.getTruncatedStackTrace(e)).build());
      responseObserver.onCompleted();
      return;
    } finally {
      // Cancel all ongoing submission
      for (CompletableFuture<?> future : submissionStubs) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
    responseObserver.onNext(
        Worker.QueryResponse.newBuilder().putMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK, "")
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void cancel(Worker.CancelRequest request, StreamObserver<Worker.CancelResponse> responseObserver) {
    try {
      _queryRunner.cancel(request.getRequestId());
    } catch (Throwable t) {
      LOGGER.error("Caught exception while cancelling opChain for request: {}", request.getRequestId(), t);
    }
    // we always return completed even if cancel attempt fails, server will self clean up in this case.
    responseObserver.onCompleted();
  }
  private SslContext buildGRpcSslContext(TlsConfig tlsConfig)
      throws Exception {
    LOGGER.info("Building gRPC SSL context");
    if (tlsConfig.getKeyStorePath() == null) {
      throw new IllegalArgumentException("Must provide key store path for secured gRpc server");
    }
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(TlsUtils.createKeyManagerFactory(tlsConfig))
        .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    if (tlsConfig.getTrustStorePath() != null) {
      sslContextBuilder.trustManager(TlsUtils.createTrustManagerFactory(tlsConfig));
    }
    if (tlsConfig.isClientAuthEnabled()) {
      sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    }
    return GrpcSslContexts.configure(sslContextBuilder).build();
  }
}
