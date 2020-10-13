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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Lists;
import com.ning.http.client.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


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

      String brokerHostPort = String.format("%s:%d", hostName, port);
      brokerList.add(brokerHostPort);
    }

    return brokerList;
  }

  public static class ControllerTenantBrokerResponseFuture extends ControllerResponseFuture<ControllerTenantBrokerResponse> {
    private final ObjectReader OBJECT_READER = new ObjectMapper().reader();

    public ControllerTenantBrokerResponseFuture(Future<Response> response, String url) {
      super(response, url);
    }

    @Override
    public ControllerTenantBrokerResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      String response = getStringResponse(timeout, unit);
      try {
        JsonNode jsonResponse = OBJECT_READER.readTree(response);
        ControllerTenantBrokerResponse tableResponse = ControllerTenantBrokerResponse.fromJson(jsonResponse);
        return tableResponse;
      } catch (IOException e) {
        new ExecutionException(e);
      }

      return null;
    }
  }
}
