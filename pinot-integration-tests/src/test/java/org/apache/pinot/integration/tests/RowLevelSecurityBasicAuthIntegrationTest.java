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
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Row Level Security integration test using BasicAuth (file-based configuration).
 * Users and permissions are configured via properties in controller and broker configuration.
 */
public class RowLevelSecurityBasicAuthIntegrationTest extends RowLevelSecurityIntegrationTestBase {

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user, user2");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user2.password", "notSoSecret");
    properties.put("controller.admin.access.control.principals.user.tables", "mytable, mytable2, mytable3");
    properties.put("controller.admin.access.control.principals.user.permissions", "read");
    properties.put("controller.admin.access.control.principals.user2.tables", "mytable, mytable2, mytable3");
    properties.put("controller.admin.access.control.principals.user2.permissions", "read");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty("pinot.broker.enable.row.column.level.auth", "true");
    brokerConf.setProperty("pinot.broker.access.control.class",
        "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    brokerConf.setProperty("pinot.broker.access.control.principals", "admin, user, user2");
    brokerConf.setProperty("pinot.broker.access.control.principals.admin.password", "verysecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.password", "secret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.password", "notSoSecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.tables", "mytable, mytable2, mytable3");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.permissions", "read");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.mytable.rls", "AirlineID='19805'");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.mytable3.rls",
        "AirlineID='20409' OR AirTime>'300', DestStateName='Florida'");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.tables", "mytable, mytable2, mytable3");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.permissions", "read");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.mytable.rls",
        "AirlineID='19805', DestStateName='California'");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.mytable2.rls",
        "AirlineID='20409', DestStateName='Florida'");
    brokerConf.setProperty("pinot.broker.access.control.principals.user2.mytable3.rls",
        "AirlineID='20409' OR DestStateName='California', DestStateName='Florida'");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.segment.fetcher.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.segment.uploader.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.instance.auth.token", AUTH_TOKEN);
  }
}
