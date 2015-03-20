/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.BrokerTestUtils;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.testng.Assert;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 *
 * @author jfim
 */
public abstract class ClusterTest extends ControllerTest {
  private static final String _success = "success";
  protected HelixBrokerStarter _brokerStarter;
  protected HelixServerStarter _offlineServerStarter;

  protected void startBroker() {
    try {
      assert _brokerStarter == null;
      Configuration configuration = BrokerTestUtils.getDefaultBrokerConfiguration();
      overrideBrokerConf(configuration);
      _brokerStarter = BrokerTestUtils.startBroker(getHelixClusterName(), ZkTestUtils.DEFAULT_ZK_STR, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startOfflineServer() {
    try {
      Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
      overrideOfflineServerConf(configuration);
      _offlineServerStarter = new HelixServerStarter(getHelixClusterName(), ZkTestUtils.DEFAULT_ZK_STR, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideOfflineServerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    BrokerTestUtils.stopBroker(_brokerStarter);
    _brokerStarter = null;
  }

  protected void stopOfflineServer() {
    // Do nothing
  }

  protected void createResource(String resourceName) throws Exception {
    JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON(resourceName, 1, 1);
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString().replaceAll("}$", ", \"metadata\":{}}"));
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));

  }

  protected void addTableToResource(String resourceName, String tableName) throws Exception {
    JSONObject payload = ControllerRequestBuilderUtil.createOfflineClusterAddTableToResource(resourceName, tableName).toJSON();
    String res =
        sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    Assert.assertEquals(_success, new JSONObject(res).getString("status"));
  }
}
