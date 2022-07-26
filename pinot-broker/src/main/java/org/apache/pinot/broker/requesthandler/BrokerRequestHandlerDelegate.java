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
import java.util.Map;
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

  private final BrokerRequestHandler _singleStageBrokerRequestHandler;
  private final MultiStageBrokerRequestHandler _multiStageWorkerRequestHandler;

  private final boolean _isMultiStageQueryEngineEnabled;


  public BrokerRequestHandlerDelegate(
      BrokerRequestHandler singleStageBrokerRequestHandler,
      @Nullable MultiStageBrokerRequestHandler multiStageWorkerRequestHandler
  ) {
    _singleStageBrokerRequestHandler = singleStageBrokerRequestHandler;
    _multiStageWorkerRequestHandler = multiStageWorkerRequestHandler;
    _isMultiStageQueryEngineEnabled = _multiStageWorkerRequestHandler != null;
  }

  @Override
  public void start() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.start();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.start();
    }
  }

  @Override
  public void shutDown() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.shutDown();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.shutDown();
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext)
      throws Exception {
    if (_isMultiStageQueryEngineEnabled && _multiStageWorkerRequestHandler != null) {
      if (request.has("queryOptions")) {
        Map<String, String> queryOptionMap = BaseBrokerRequestHandler.getOptionsFromJson(request, "queryOptions");
        if (Boolean.parseBoolean(queryOptionMap.get(
            CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE))) {
          return _multiStageWorkerRequestHandler.handleRequest(request, requesterIdentity, requestContext);
        }
      }
    }
    return _singleStageBrokerRequestHandler.handleRequest(request, requesterIdentity, requestContext);
  }
}
