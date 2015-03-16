package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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
      Configuration configuration = DefaultHelixBrokerConfig.getDefaultBrokerConf();
      overrideBrokerConf(configuration);
      _brokerStarter = new HelixBrokerStarter(getHelixClusterName(), ZK_STR, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startOfflineServer() {
    try {
      Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
      overrideOfflineServerConf(configuration);
      _offlineServerStarter = new HelixServerStarter(getHelixClusterName(), ZK_STR, configuration);
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
    try {
      _brokerStarter.getBrokerServerBuilder().stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
