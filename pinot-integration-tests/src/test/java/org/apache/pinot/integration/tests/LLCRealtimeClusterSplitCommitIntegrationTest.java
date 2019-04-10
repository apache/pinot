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

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;


/**
 * Integration test that extends LLCRealtimeClusterIntegrationTest but with split commit enabled.
 */
public class LLCRealtimeClusterSplitCommitIntegrationTest extends LLCRealtimeClusterIntegrationTest {
  @Override
  public void startController() {
    ControllerConf controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.setSplitCommit(true);
    startController(controllerConfig);
  }

  @Override
  public void startServer() {
    Configuration serverConfig = getDefaultServerConfiguration();
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
    startServer(serverConfig);
  }
}
