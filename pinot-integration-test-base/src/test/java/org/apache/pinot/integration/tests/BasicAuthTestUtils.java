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
package org.apache.pinot.integration.tests;

import java.util.Map;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.spi.env.PinotConfiguration;


public final class BasicAuthTestUtils {
  private BasicAuthTestUtils() {
  }

  public static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=====";
  public static final String AUTH_TOKEN_USER = "Basic dXNlcjpzZWNyZXQ==";
  public static final Map<String, String> AUTH_HEADER = Map.of("Authorization", AUTH_TOKEN);
  public static final BasicHeader AUTH_HEADER_BASIC = new BasicHeader("Authorization", AUTH_TOKEN);
  public static final Map<String, String> AUTH_HEADER_USER = Map.of("Authorization", AUTH_TOKEN_USER);

  public static void addControllerConfiguration(Map<String, Object> properties) {
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.tables", "userTableOnly");
    properties.put("controller.admin.access.control.principals.user.permissions", "read");
  }

  public static void addBrokerConfiguration(PinotConfiguration brokerConf) {
    brokerConf.setProperty("pinot.broker.access.control.class",
        "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    brokerConf.setProperty("pinot.broker.access.control.principals", "admin, user");
    brokerConf.setProperty("pinot.broker.access.control.principals.admin.password", "verysecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.password", "secret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.tables", "userTableOnly");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.permissions", "read");
  }

  public static void addServerConfiguration(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.segment.fetcher.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.segment.uploader.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.instance.auth.token", AUTH_TOKEN);
  }

  public static void addMinionConfiguration(PinotConfiguration minionConf) {
    minionConf.setProperty("segment.fetcher.auth.token", AUTH_TOKEN);
    minionConf.setProperty("task.auth.token", AUTH_TOKEN);
  }
}
