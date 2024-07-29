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
package org.apache.pinot.client.controller.response;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.JsonUtils;
import org.asynchttpclient.Response;


public class ControllerTenantBrokerResponse {
  private JsonNode _brokers;

  private ControllerTenantBrokerResponse() {
  }

  private ControllerTenantBrokerResponse(JsonNode controllerTenantBrokerResponse) {
    _brokers = controllerTenantBrokerResponse;
  }

  public static ControllerTenantBrokerResponse fromJson(JsonNode controllerTenantBrokerResponse) {
    return new ControllerTenantBrokerResponse(controllerTenantBrokerResponse);
  }

  public static ControllerTenantBrokerResponse empty() {
    return new ControllerTenantBrokerResponse();
  }

  public List<String> getBrokers() {
    List<String> brokerList = new ArrayList<>();

    for (JsonNode broker : _brokers) {
      String hostName = broker.get("host").textValue();
      Integer port = broker.get("port").intValue();
      String brokerIP = hostName;
      if (hostName.contains("_")) {
        String[] hostNamePart = hostName.split("_");
        brokerIP = hostNamePart[hostNamePart.length - 1];
      }
      String brokerIPPort = String.format("%s:%d", brokerIP, port);
      brokerList.add(brokerIPPort);
    }

    return brokerList;
  }

  public static class ControllerTenantBrokerResponseFuture
      extends ControllerResponseFuture<ControllerTenantBrokerResponse> {
    public ControllerTenantBrokerResponseFuture(Future<Response> response, String url) {
      super(response, url);
    }

    @Override
    public ControllerTenantBrokerResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      try {
        InputStream response = getStreamResponse(timeout, unit);
        JsonNode jsonResponse = JsonUtils.inputStreamToJsonNode(response);
        return ControllerTenantBrokerResponse.fromJson(jsonResponse);
      } catch (IOException e) {
        throw new ExecutionException(e);
      }
    }
  }
}
