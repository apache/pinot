/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TagNameUtils;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerTenantTest extends ControllerTest {
  private static final String BROKER_TAG_PREFIX = "brokerTag_";
  private static final String SERVER_TAG_PREFIX = "serverTag_";
  private static final int NUM_INSTANCES = 10;
  private static final int NUM_BROKER_TAGS = 3;
  private static final int NUM_BROKERS_PER_TAG = 3;
  private static final int NUM_SERVER_TAGS = 2;
  private static final int NUM_OFFLINE_SERVERS_PER_TAG = 2;
  private static final int NUM_REALTIME_SERVERS_PER_TAG = 1;
  private static final int NUM_SERVERS_PER_TAG = NUM_OFFLINE_SERVERS_PER_TAG + NUM_REALTIME_SERVERS_PER_TAG;

  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, false);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName,
        ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR,
        NUM_INSTANCES);
  }

  @Test
  public void testBrokerTenant() throws IOException, JSONException {
    // Create broker tenants
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + i;
      JSONObject payload =
          ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTenant, NUM_BROKERS_PER_TAG);
      sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(), payload.toString());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(), NUM_BROKERS_PER_TAG);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), NUM_INSTANCES - i * NUM_BROKERS_PER_TAG);
    }

    // Get broker tenants
    JSONObject response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.getJSONArray("BROKER_TENANTS").length(), NUM_BROKER_TAGS);
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTag = BROKER_TAG_PREFIX + i;
      response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTenantGet(brokerTag)));
      Assert.assertEquals(response.getJSONArray("BrokerInstances").length(), NUM_BROKERS_PER_TAG);
      Assert.assertEquals(response.getString("tenantName"), brokerTag);
      response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTenantGet(brokerTag)));
      Assert.assertEquals(response.getJSONArray("BrokerInstances").length(), NUM_BROKERS_PER_TAG);
      Assert.assertEquals(response.getJSONArray("ServerInstances").length(), 0);
      Assert.assertEquals(response.getString("tenantName"), brokerTag);
    }

    // Update broker tenants
    for (int i = 0; i <= NUM_INSTANCES - (NUM_BROKER_TAGS - 1) * NUM_BROKERS_PER_TAG; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + 1;
      JSONObject payload = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTenant, i);
      sendPutRequest(_controllerRequestURLBuilder.forTenantCreate(), payload.toString());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(), i);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), NUM_INSTANCES - (NUM_BROKER_TAGS - 1) * NUM_BROKERS_PER_TAG - i);
    }

    // Delete broker tenants
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + i;
      sendDeleteRequest(_controllerRequestURLBuilder.forBrokerTenantDelete(brokerTenant));
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(), 0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), NUM_INSTANCES - (NUM_BROKER_TAGS - i) * NUM_BROKERS_PER_TAG);
    }
  }

  @Test
  public void testServerTenant() throws IOException, JSONException {
    // Create server tenants
    for (int i = 1; i <= NUM_SERVER_TAGS; i++) {
      String serverTenant = SERVER_TAG_PREFIX + i;
      JSONObject payload =
          ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTenant, NUM_SERVERS_PER_TAG,
              NUM_OFFLINE_SERVERS_PER_TAG, NUM_REALTIME_SERVERS_PER_TAG);
      sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(), payload.toString());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getOfflineTagForTenant(serverTenant)).size(), NUM_OFFLINE_SERVERS_PER_TAG);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getRealtimeTagForTenant(serverTenant)).size(), NUM_REALTIME_SERVERS_PER_TAG);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), NUM_INSTANCES - i * NUM_SERVERS_PER_TAG);
    }

    // Get server tenants
    JSONObject response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.getJSONArray("SERVER_TENANTS").length(), NUM_SERVER_TAGS);
    for (int i = 1; i <= NUM_SERVER_TAGS; i++) {
      String serverTag = SERVER_TAG_PREFIX + i;
      response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forServerTenantGet(serverTag)));
      Assert.assertEquals(response.getJSONArray("ServerInstances").length(), NUM_SERVERS_PER_TAG);
      Assert.assertEquals(response.getString("tenantName"), serverTag);
      response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTenantGet(serverTag)));
      Assert.assertEquals(response.getJSONArray("BrokerInstances").length(), 0);
      Assert.assertEquals(response.getJSONArray("ServerInstances").length(), NUM_SERVERS_PER_TAG);
      Assert.assertEquals(response.getString("tenantName"), serverTag);
    }

    // Update server tenants
    // Note: server tenants cannot scale down
    for (int i = 0; i <= (NUM_INSTANCES - NUM_SERVER_TAGS * NUM_SERVERS_PER_TAG) / 2; i++) {
      String serverTenant = SERVER_TAG_PREFIX + 1;
      JSONObject payload =
          ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTenant, NUM_SERVERS_PER_TAG + i * 2,
              NUM_OFFLINE_SERVERS_PER_TAG + i, NUM_REALTIME_SERVERS_PER_TAG + i);
      sendPutRequest(_controllerRequestURLBuilder.forTenantCreate(), payload.toString());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getOfflineTagForTenant(serverTenant)).size(),
          NUM_OFFLINE_SERVERS_PER_TAG + i);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getRealtimeTagForTenant(serverTenant)).size(),
          NUM_REALTIME_SERVERS_PER_TAG + i);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), NUM_INSTANCES - NUM_SERVER_TAGS * NUM_SERVERS_PER_TAG - i * 2);
    }

    // Delete server tenants
    for (int i = 1; i < NUM_SERVER_TAGS; i++) {
      String serverTenant = SERVER_TAG_PREFIX + i;
      sendDeleteRequest(_controllerRequestURLBuilder.forServerTenantDelete(serverTenant));
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getOfflineTagForTenant(serverTenant)).size(), 0);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
          TagNameUtils.getRealtimeTagForTenant(serverTenant)).size(), 0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), NUM_INSTANCES - (NUM_SERVER_TAGS - i) * NUM_SERVERS_PER_TAG);
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
