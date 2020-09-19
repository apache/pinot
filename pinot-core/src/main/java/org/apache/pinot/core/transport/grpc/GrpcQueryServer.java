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
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.operator.streaming.StreamingResponseUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Plug in QueryScheduler
public class GrpcQueryServer extends PinotQueryServerGrpc.PinotQueryServerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryServer.class);

  private final QueryExecutor _queryExecutor;
  private final ServerMetrics _serverMetrics;
  private final Server _server;
  private final ExecutorService _executorService =
      Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS);

  public GrpcQueryServer(int port, QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
    _server = ServerBuilder.forPort(port).addService(this).build();
    LOGGER.info("Initialized GrpcQueryServer on port: {} with numWorkerThreads: {}", port,
        ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
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

    // Process the query
    DataTable dataTable;
    try {
      dataTable = _queryExecutor.processQuery(queryRequest, _executorService, responseObserver);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing request {}: {} from broker: {}", queryRequest.getRequestId(),
          queryRequest.getQueryContext(), queryRequest.getBrokerId(), e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
      return;
    }

    ServerResponse response;
    try {
      response = queryRequest.isEnableStreaming() ? StreamingResponseUtils.getMetadataResponse(dataTable)
          : StreamingResponseUtils.getNonStreamingResponse(dataTable);
    } catch (Exception e) {
      LOGGER.error("Caught exception while constructing response from data table for request {}: {} from broker: {}",
          queryRequest.getRequestId(), queryRequest.getQueryContext(), queryRequest.getBrokerId(), e);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
      return;
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
