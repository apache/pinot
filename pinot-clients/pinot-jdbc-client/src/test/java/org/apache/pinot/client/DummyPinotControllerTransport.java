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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.controller.PinotControllerTransport;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;
import org.apache.pinot.spi.utils.JsonUtils;


public class DummyPinotControllerTransport extends PinotControllerTransport {

  public DummyPinotControllerTransport(Map<String, String> headers, String scheme, @Nullable SSLContext sslContext,
      @Nullable String appId) {
    super(headers, scheme, sslContext, ConnectionTimeouts.create(1000, 1000, 1000), TlsProtocols.defaultProtocols(true),
        appId);
  }

  @Override
  public ControllerTenantBrokerResponse getBrokersFromController(String controllerAddress, String tenant) {
    try {
      String jsonString = "[{\"instanceName\": \"dummy\", \"host\" : \"dummy\", \"port\" : 8000}]";
      JsonNode dummyBrokerJsonResponse = JsonUtils.stringToJsonNode(jsonString);
      return ControllerTenantBrokerResponse.fromJson(dummyBrokerJsonResponse);
    } catch (Exception e) {
    }
    return ControllerTenantBrokerResponse.empty();
  }

  public static DummyPinotControllerTransport create() {
    return create("dummy");
  }

  public static DummyPinotControllerTransport create(String appId) {
    return new DummyPinotControllerTransport(null, null, null, appId);
  }
}
