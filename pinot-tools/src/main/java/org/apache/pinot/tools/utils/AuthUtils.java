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
package org.apache.pinot.tools.utils;

import java.util.HashMap;
import java.util.Map;


public class AuthUtils {
  private static final String DEFAULT_AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA==";

  private AuthUtils() {
  }

  public static Map<String, Object> getAuthQuickStartDefaultConfigs() {
    Map<String, Object> properties = new HashMap<>();
    // controller
    properties.put("pinot.controller.segment.fetcher.auth.token", DEFAULT_AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
      "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user, service, tableonly");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.service.password", "verysecrettoo");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.permissions", "READ");
    properties.put("controller.admin.access.control.principals.tableonly.password", "secrettoo");
    properties.put("controller.admin.access.control.principals.tableonly.permissions", "READ");
    properties.put("controller.admin.access.control.principals.tableonly.tables", "baseballStats");

    // broker
    properties.put("pinot.broker.access.control.class", "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    properties.put("pinot.broker.access.control.principals", "admin, user, service, tableonly");
    properties.put("pinot.broker.access.control.principals.admin.password", "verysecret");
    properties.put("pinot.broker.access.control.principals.service.password", "verysecrettoo");
    properties.put("pinot.broker.access.control.principals.user.password", "secret");
    properties.put("pinot.broker.access.control.principals.tableonly.password", "secrettoo");
    properties.put("pinot.broker.access.control.principals.tableonly.tables", "baseballStats");

    // server
    properties.put("pinot.server.segment.fetcher.auth.token", DEFAULT_AUTH_TOKEN);
    properties.put("pinot.server.segment.uploader.auth.token", DEFAULT_AUTH_TOKEN);
    properties.put("pinot.server.instance.auth.token", DEFAULT_AUTH_TOKEN);

    // minion
    properties.put("segment.fetcher.auth.token", DEFAULT_AUTH_TOKEN);
    properties.put("task.auth.token", DEFAULT_AUTH_TOKEN);

    // loggers
    properties.put("pinot.controller.logger.root.dir", "logs");
    properties.put("pinot.broker.logger.root.dir", "logs");
    properties.put("pinot.server.logger.root.dir", "logs");
    properties.put("pinot.minion.logger.root.dir", "logs");
    return properties;
  }
}
