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
package org.apache.pinot.controller.api.resources;

import java.util.Map;
import org.apache.pinot.controller.helix.ControllerRequestClient;


public class PinotWithAuthLogicalTableResourceTest extends PinotLogicalTableResourceTest {

  private static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=====";
  public static final Map<String, String> AUTH_HEADER = Map.of("Authorization", AUTH_TOKEN);

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
  }

  @Override
  protected Map<String, String> getHeaders() {
    return AUTH_HEADER;
  }

  @Override
  public ControllerRequestClient getControllerRequestClient() {
    if (_controllerRequestClient == null) {
      _controllerRequestClient =
          new ControllerRequestClient(_controllerRequestURLBuilder, getHttpClient(), AUTH_HEADER);
    }
    return _controllerRequestClient;
  }
}
