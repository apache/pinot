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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.auth.BasicAuthUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.plugin.PluginManager;


public class AuthZkBasicQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Collections.singletonList("AUTH-ZK");
  }

  @Override
  public AuthProvider getAuthProvider() {
    return AuthProviderUtils.makeAuthProvider(BasicAuthUtils.toBasicAuthToken("admin", "verysecret"));
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> properties = new HashMap<>(super.getConfigOverrides());

    // controller
    properties.put("pinot.controller.segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.ZkBasicAuthAccessControlFactory");
    properties.put("access.control.init.username", "admin");
    properties.put("access.control.init.password", "verysecret");

    // broker
    properties.put("pinot.broker.access.control.class",
        "org.apache.pinot.broker.broker.ZkBasicAuthAccessControlFactory");

    // server
    properties.put("pinot.server.segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("pinot.server.segment.uploader.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");

    // minion
    properties.put("segment.fetcher.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");
    properties.put("task.auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");

    return properties;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    new AuthZkBasicQuickstart().execute();
    printStatus(Color.GREEN, "Default login credential to login controller is admin/verysecret.");
  }
}
