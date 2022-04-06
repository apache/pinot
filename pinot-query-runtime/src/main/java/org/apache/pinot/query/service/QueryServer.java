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

import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryServer} is the GRPC server that accepts query plan requests sent from {@link QueryDispatcher}.
 */
public class QueryServer extends PinotQueryWorkerGrpc.PinotQueryWorkerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryServer.class);

  private final Server _server;
  private final QueryRunner _queryRunner;
  private final ExecutorService _executorService;

  public QueryServer(int port, QueryRunner queryRunner) {
    _server = ServerBuilder.forPort(port).addService(this).build();
    _executorService = Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS,
        new NamedThreadFactory("query_worker_on_" + port + "_port"));
    _queryRunner = queryRunner;
    LOGGER.info("Initialized QueryWorker on port: {} with numWorkerThreads: {}", port,
        ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  }

  public void start() {
    LOGGER.info("Starting QueryWorker");
    try {
      _queryRunner.start();
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down QueryWorker");
    try {
      _queryRunner.shutDown();
      _server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
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

    // return dispatch successful.
    // TODO: return meaningful value here.
    responseObserver.onNext(Worker.QueryResponse.newBuilder().putMetadata("OK", "OK").build());
    responseObserver.onCompleted();

    // start a new GRPC ctx has all the values as the current context, but won't be cancelled
    Context ctx = Context.current().fork();
    // Set ctx as the current context within the Runnable can start asynchronous work here that will not
    // be cancelled when submit returns
    ctx.run(() -> {
      // Process the query
      try {
        // TODO: break this into parsing and execution, so that responseObserver can return upon parsing complete.
        _queryRunner.processQuery(distributedStagePlan, _executorService, requestMetadataMap);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing request", e);
        throw new RuntimeException(e);
      }
    });
  }
}
