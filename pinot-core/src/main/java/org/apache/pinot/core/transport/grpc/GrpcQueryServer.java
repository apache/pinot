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
package org.apache.pinot.core.transport.grpc;

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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import nl.altindag.ssl.SSLFactory;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.streaming.StreamingResponseUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.GrpcRequesterIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Plug in QueryScheduler
public class GrpcQueryServer extends PinotQueryServerGrpc.PinotQueryServerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryServer.class);
  // the key is the hashCode of the TlsConfig, the value is the SslContext
  private static final Map<Integer, SslContext> SERVER_SSL_CONTEXTS_CACHE = new ConcurrentHashMap<>();

  private final QueryExecutor _queryExecutor;
  private final ServerMetrics _serverMetrics;
  private final Server _server;
  private final ExecutorService _executorService =
      Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  private final AccessControl _accessControl;
  private final ServerQueryLogger _queryLogger = ServerQueryLogger.getInstance();

  public GrpcQueryServer(int port, GrpcConfig config, TlsConfig tlsConfig, QueryExecutor queryExecutor,
      ServerMetrics serverMetrics, AccessControl accessControl) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
    if (tlsConfig != null) {
      try {
        _server = NettyServerBuilder.forPort(port).sslContext(buildGRpcSslContext(tlsConfig))
            .maxInboundMessageSize(config.getMaxInboundMessageSizeBytes()).addService(this).build();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start secure grpcQueryServer", e);
      }
    } else {
      _server = ServerBuilder.forPort(port).addService(this).build();
    }
    _accessControl = accessControl;
    LOGGER.info("Initialized GrpcQueryServer on port: {} with numWorkerThreads: {}", port,
        ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  }

  private SslContext buildGRpcSslContext(TlsConfig tlsConfig)
      throws IllegalArgumentException {
    LOGGER.info("Building gRPC SSL context");
    if (tlsConfig.getKeyStorePath() == null) {
      throw new IllegalArgumentException("Must provide key store path for secured gRpc server");
    }
    SslContext sslContext = SERVER_SSL_CONTEXTS_CACHE.computeIfAbsent(tlsConfig.hashCode(), tlsConfigHashCode -> {
      try {
        SSLFactory sslFactory = TlsUtils.createSSLFactory(tlsConfig);
        if (TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getKeyStorePath())
            && TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getTrustStorePath())) {
          TlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, tlsConfig);
        }
        SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(sslFactory.getKeyManagerFactory().get())
            .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
        sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);
        if (tlsConfig.isClientAuthEnabled()) {
          sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        }
        return GrpcSslContexts.configure(sslContextBuilder).build();
      } catch (Exception e) {
        throw new RuntimeException("Failed to build gRPC SSL context", e);
      }
    });
    return sslContext;
  }

  public void start() {
    LOGGER.info("Starting GrpcQueryServer");
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down GrpcQueryServer");
    try {
      _server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void submit(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
    _serverMetrics.addMeteredGlobalValue(ServerMeter.GRPC_QUERIES, 1);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.GRPC_BYTES_RECEIVED, request.getSerializedSize());

    // Deserialize the request
    ServerQueryRequest queryRequest;
    try {
      queryRequest = new ServerQueryRequest(request, _serverMetrics);
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing the request: {}", request, e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad request").withCause(e).asException());
      return;
    }

    // Table level access control
    GrpcRequesterIdentity requestIdentity = new GrpcRequesterIdentity(request.getMetadataMap());
    if (!_accessControl.hasDataAccess(requestIdentity, queryRequest.getTableNameWithType())) {
      Exception unsupportedOperationException = new UnsupportedOperationException(
          String.format("No access to table %s while processing request %d: %s from broker: %s",
              queryRequest.getTableNameWithType(), queryRequest.getRequestId(), queryRequest.getQueryContext(),
              queryRequest.getBrokerId()));
      final String exceptionMsg = String.format("Table not found: %s", queryRequest.getTableNameWithType());
      LOGGER.error(exceptionMsg, unsupportedOperationException);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.NO_TABLE_ACCESS, 1);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(exceptionMsg).withCause(unsupportedOperationException).asException());
    }

    // Process the query
    InstanceResponseBlock instanceResponse;
    try {
      instanceResponse =
          _queryExecutor.execute(queryRequest, _executorService, new GrpcResultsBlockStreamer(responseObserver));
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing request {}: {} from broker: {}", queryRequest.getRequestId(),
          queryRequest.getQueryContext(), queryRequest.getBrokerId(), e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
      return;
    }

    ServerResponse serverResponse;
    try {
      DataTable dataTable = instanceResponse.toDataTable();
      serverResponse = queryRequest.isEnableStreaming() ? StreamingResponseUtils.getMetadataResponse(dataTable)
          : StreamingResponseUtils.getNonStreamingResponse(dataTable);
    } catch (Exception e) {
      LOGGER.error("Caught exception while serializing response for request {}: {} from broker: {}",
          queryRequest.getRequestId(), queryRequest.getQueryContext(), queryRequest.getBrokerId(), e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
      return;
    }
    responseObserver.onNext(serverResponse);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.GRPC_BYTES_SENT, serverResponse.getSerializedSize());
    responseObserver.onCompleted();

    // Log the query
    if (_queryLogger != null) {
      _queryLogger.logQuery(queryRequest, instanceResponse, "GrpcQueryServer");
    }
  }
}
