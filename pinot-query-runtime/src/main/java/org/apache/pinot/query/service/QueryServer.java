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
package org.apache.pinot.query.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryServer} is the GRPC server that accepts query plan requests sent from {@link QueryDispatcher}.
 */
public class QueryServer extends PinotQueryWorkerGrpc.PinotQueryWorkerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryServer.class);
  // TODO: Inbound messages can get quite large because we send the entire stage metadata map in each call.
  // See https://github.com/apache/pinot/issues/10331
  private static final int MAX_INBOUND_MESSAGE_SIZE = 64 * 1024 * 1024;

  private final int _port;
  private Server _server = null;
  private final QueryRunner _queryRunner;
  private final ExecutorService _executorService;

  public QueryServer(int port, QueryRunner queryRunner) {
    _port = port;
    _queryRunner = queryRunner;
    _executorService = queryRunner.getQueryRunnerExecutorService();
  }

  public void start() {
    LOGGER.info("Starting QueryServer");
    try {
      if (_server == null) {
        _server = ServerBuilder.forPort(_port).addService(this).maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE).build();
        LOGGER.info("Initialized QueryServer on port: {}", _port);
      }
      _queryRunner.start();
      _server.start();
    } catch (IOException | TimeoutException e) {
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
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void submit(Worker.QueryRequest request, StreamObserver<Worker.QueryResponse> responseObserver) {
    // Deserialize the request
    DistributedStagePlan distributedStagePlan;
    Map<String, String> requestMetadataMap;
    try {
      distributedStagePlan = QueryPlanSerDeUtils.deserialize(request.getStagePlan());
      requestMetadataMap = request.getMetadataMap();
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing the request: {}", request, e);
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad request").withCause(e).asException());
      return;
    }

    // TODO: break this into parsing and execution, so that responseObserver can return upon compilation complete.
    // compilation complete indicates dispatch successful.
    _executorService.submit(() -> _queryRunner.processQuery(distributedStagePlan, requestMetadataMap));

    responseObserver.onNext(Worker.QueryResponse.newBuilder()
        .putMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_OK, "").build());
    responseObserver.onCompleted();
  }

  @Override
  public void cancel(Worker.CancelRequest request, StreamObserver<Worker.CancelResponse> responseObserver) {
    _queryRunner.cancel(request.getRequestId());
    responseObserver.onCompleted();
  }
}
