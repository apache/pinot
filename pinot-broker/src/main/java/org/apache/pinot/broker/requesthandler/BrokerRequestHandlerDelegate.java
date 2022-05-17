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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nullable;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * {@code BrokerRequestHandlerDelegate} delegates the inbound broker request to one of the enabled
 * {@link BrokerRequestHandler} based on the requested handle type.
 *
 * {@see: @CommonConstant
 */
public class BrokerRequestHandlerDelegate implements BrokerRequestHandler {

  @Nullable
  private SingleConnectionBrokerRequestHandler _singleConnectionBrokerRequestHandler;
  @Nullable
  private GrpcBrokerRequestHandler _grpcBrokerRequestHandler;
  @Nullable
  private WorkerQueryRequestHandler _multiStageWorkerRequestHandler;

  public BrokerRequestHandlerDelegate(
      @Nullable SingleConnectionBrokerRequestHandler singleConnectionBrokerRequestHandler,
      @Nullable GrpcBrokerRequestHandler grpcBrokerRequestHandler,
      @Nullable WorkerQueryRequestHandler multiStageWorkerRequestHandler
  ) {
    _singleConnectionBrokerRequestHandler = singleConnectionBrokerRequestHandler;
    _grpcBrokerRequestHandler = grpcBrokerRequestHandler;
    _multiStageWorkerRequestHandler = multiStageWorkerRequestHandler;
  }

  @Override
  public void start() {
    if (_singleConnectionBrokerRequestHandler != null) {
      _singleConnectionBrokerRequestHandler.start();
    }
    if (_grpcBrokerRequestHandler != null) {
      _grpcBrokerRequestHandler.start();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.start();
    }
  }

  @Override
  public void shutDown() {
    if (_singleConnectionBrokerRequestHandler != null) {
      _singleConnectionBrokerRequestHandler.shutDown();
    }
    if (_grpcBrokerRequestHandler != null) {
      _grpcBrokerRequestHandler.shutDown();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.shutDown();
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext)
      throws Exception {
    JsonNode node = request.get(CommonConstants.Broker.BROKER_REQUEST_HANDLER_TYPE_JSON_OVERRIDE_KEY);
    String handlerType = node == null ? CommonConstants.Broker.DEFAULT_BROKER_REQUEST_HANDLER_TYPE : node.asText();
    switch (handlerType) {
      case CommonConstants.Broker.NETTY_BROKER_REQUEST_HANDLER_TYPE:
        if (_singleConnectionBrokerRequestHandler != null) {
          return _singleConnectionBrokerRequestHandler.handleRequest(request, requesterIdentity, requestContext);
        } else {
          break;
        }
      case CommonConstants.Broker.GRPC_BROKER_REQUEST_HANDLER_TYPE:
        if (_grpcBrokerRequestHandler != null) {
          return _grpcBrokerRequestHandler.handleRequest(request, requesterIdentity, requestContext);
        } else {
          break;
        }
      case CommonConstants.Broker.MULTI_STAGE_BROKER_REQUEST_HANDLER_TYPE:
        if (_multiStageWorkerRequestHandler != null) {
          return _multiStageWorkerRequestHandler.handleRequest(request, requesterIdentity, requestContext);
        } else {
          break;
        }
      default:
        break;
    }
    throw new UnsupportedOperationException("Unable to find request handler for type: " + handlerType);
  }
}
