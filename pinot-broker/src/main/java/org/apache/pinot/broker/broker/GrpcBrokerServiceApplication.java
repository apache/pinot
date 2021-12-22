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
package org.apache.pinot.broker.broker;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.broker.api.GrpcRequesterIdentity;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.requesthandler.GrpcBrokerRequestHandler;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.proto.PinotStreamingQueryServiceGrpc;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcBrokerServiceApplication extends PinotStreamingQueryServiceGrpc.PinotStreamingQueryServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcBrokerServiceApplication.class);
  private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;

  private final Server _server;
  private RoutingManager _routingManager;
  private GrpcBrokerRequestHandler _grpcBrokerRequestHandler;
  private BrokerMetrics _brokerMetrics;
  private PinotConfiguration _brokerConf;

  public GrpcBrokerServiceApplication(RoutingManager routingManager, GrpcBrokerRequestHandler grpcBrokerRequestHandler,
      BrokerMetrics brokerMetrics, PinotConfiguration brokerConf) {
    _routingManager = routingManager;
    _grpcBrokerRequestHandler = grpcBrokerRequestHandler;
    _brokerMetrics = brokerMetrics;
    _brokerConf = brokerConf;
    _server = ServerBuilder
        .forPort(brokerConf.getProperty(CommonConstants.Helix.KEY_OF_BROKER_STREAMING_QUERY_PORT,
            CommonConstants.Helix.DEFAULT_BROKER_STREAMING_QUERY_PORT))
        .maxInboundMessageSize(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE)
        .addService(this)
        .build();
  }

  public void start() {
    LOGGER.info("Starting GrpcBrokerQueryService");
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down GrpcBrokerQueryService");
    try {
      _server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void submit(Broker.BrokerRequest request, StreamObserver<Broker.BrokerResponse> responseObserver) {
    try {
      BrokerResponseNative brokerResponseNative =
          _grpcBrokerRequestHandler.handleRequestInternal(JsonUtils.stringToJsonNode(request.getBrokerRequest()),
              new GrpcRequesterIdentity(), new RequestStatistics(), responseObserver);
      responseObserver.onNext(Broker.BrokerResponse.newBuilder()
          .setPayload(ByteString.copyFrom(brokerResponseNative.toJsonString().getBytes(StandardCharsets.UTF_8)))
          .putMetadata("TYPE", "METADATA")
          .build());
    } catch (Exception e) {
      LOGGER.error("error", e);
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }
}
