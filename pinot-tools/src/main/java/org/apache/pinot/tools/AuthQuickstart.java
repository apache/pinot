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
package org.apache.pinot.tools;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.plugin.PluginManager;


public class AuthQuickstart extends Quickstart {

  @Override
  public String getAuthToken() {
    return BasicAuthUtils.toBasicAuthToken("admin", "verysecret");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> properties = new HashMap<>();

    // controller
    properties.put("controller.segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("controller.admin.access.control.factory.class", "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.tables", "baseballStats");
    properties.put("controller.admin.access.control.principals.user.permissions", "read");

    // broker
    properties.put("pinot.broker.access.control.class", "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    properties.put("pinot.broker.access.control.principals", "admin, user");
    properties.put("pinot.broker.access.control.principals.admin.password", "verysecret");
    properties.put("pinot.broker.access.control.principals.user.password", "secret");
    properties.put("pinot.broker.access.control.principals.user.tables", "baseballStats");
    properties.put("pinot.broker.access.control.principals.user.permissions", "read");

    // server
    properties.put("pinot.server.segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("pinot.server.segment.upload.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("pinot.server.instance.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");

    // minion
    properties.put("segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("task.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");

    return properties;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new AuthQuickstart().execute();
  }
}
