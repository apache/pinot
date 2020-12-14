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
package org.apache.pinot.controller.helix;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerTenantStatelessTest extends ControllerTest {
  private static final String BROKER_TAG_PREFIX = "brokerTag_";
  private static final String SERVER_TAG_PREFIX = "serverTag_";
  private static final int NUM_INSTANCES = 10;
  private static final int NUM_BROKER_TAGS = 3;
  private static final int NUM_BROKERS_PER_TAG = 3;
  private static final int NUM_SERVER_TAGS = 2;
  private static final int NUM_OFFLINE_SERVERS_PER_TAG = 2;
  private static final int NUM_REALTIME_SERVERS_PER_TAG = 1;
  private static final int NUM_SERVERS_PER_TAG = NUM_OFFLINE_SERVERS_PER_TAG + NUM_REALTIME_SERVERS_PER_TAG;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, false);
    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false);
  }

  @Test
  public void testBrokerTenant()
      throws IOException {
    // Create broker tenants
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + i;
      createBrokerTenant(brokerTenant, NUM_BROKERS_PER_TAG);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(),
          NUM_BROKERS_PER_TAG);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size(),
          NUM_INSTANCES - i * NUM_BROKERS_PER_TAG);
    }

    // Get broker tenants
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.get("BROKER_TENANTS").size(), NUM_BROKER_TAGS);
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTag = BROKER_TAG_PREFIX + i;
      response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forBrokerTenantGet(brokerTag)));
      Assert.assertEquals(response.get("BrokerInstances").size(), NUM_BROKERS_PER_TAG);
      Assert.assertEquals(response.get("tenantName").asText(), brokerTag);
      response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet(brokerTag)));
      Assert.assertEquals(response.get("BrokerInstances").size(), NUM_BROKERS_PER_TAG);
      Assert.assertEquals(response.get("ServerInstances").size(), 0);
      Assert.assertEquals(response.get("tenantName").asText(), brokerTag);
    }

    // Update broker tenants
    for (int i = 0; i <= NUM_INSTANCES - (NUM_BROKER_TAGS - 1) * NUM_BROKERS_PER_TAG; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + 1;
      updateBrokerTenant(brokerTenant, i);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(),
          i);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size(),
          NUM_INSTANCES - (NUM_BROKER_TAGS - 1) * NUM_BROKERS_PER_TAG - i);
    }

    // Delete broker tenants
    for (int i = 1; i <= NUM_BROKER_TAGS; i++) {
      String brokerTenant = BROKER_TAG_PREFIX + i;
      sendDeleteRequest(_controllerRequestURLBuilder.forBrokerTenantDelete(brokerTenant));
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getBrokerTagForTenant(brokerTenant)).size(),
          0);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size(),
          NUM_INSTANCES - (NUM_BROKER_TAGS - i) * NUM_BROKERS_PER_TAG);
    }
  }

  @Test
  public void testEmptyServerTenant() {
    try {
      sendGetRequest(_controllerRequestURLBuilder.forServerTenantGet("doesn't_exist"));
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testServerTenant()
      throws IOException {
    // Create server tenants
    for (int i = 1; i <= NUM_SERVER_TAGS; i++) {
      String serverTenant = SERVER_TAG_PREFIX + i;
      createServerTenant(serverTenant, NUM_OFFLINE_SERVERS_PER_TAG, NUM_REALTIME_SERVERS_PER_TAG);
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getOfflineTagForTenant(serverTenant))
          .size(), NUM_OFFLINE_SERVERS_PER_TAG);
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getRealtimeTagForTenant(serverTenant))
          .size(), NUM_REALTIME_SERVERS_PER_TAG);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE).size(),
          NUM_INSTANCES - i * NUM_SERVERS_PER_TAG);
    }

    // Get server tenants
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.get("SERVER_TENANTS").size(), NUM_SERVER_TAGS);
    for (int i = 1; i <= NUM_SERVER_TAGS; i++) {
      String serverTag = SERVER_TAG_PREFIX + i;
      response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forServerTenantGet(serverTag)));
      Assert.assertEquals(response.get("ServerInstances").size(), NUM_SERVERS_PER_TAG);
      Assert.assertEquals(response.get("tenantName").asText(), serverTag);
      response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet(serverTag)));
      Assert.assertEquals(response.get("BrokerInstances").size(), 0);
      Assert.assertEquals(response.get("ServerInstances").size(), NUM_SERVERS_PER_TAG);
      Assert.assertEquals(response.get("tenantName").asText(), serverTag);
    }

    // Update server tenants
    // Note: server tenants cannot scale down
    for (int i = 0; i <= (NUM_INSTANCES - NUM_SERVER_TAGS * NUM_SERVERS_PER_TAG) / 2; i++) {
      String serverTenant = SERVER_TAG_PREFIX + 1;
      updateServerTenant(serverTenant, NUM_OFFLINE_SERVERS_PER_TAG + i, NUM_REALTIME_SERVERS_PER_TAG + i);
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getOfflineTagForTenant(serverTenant))
          .size(), NUM_OFFLINE_SERVERS_PER_TAG + i);
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getRealtimeTagForTenant(serverTenant))
          .size(), NUM_REALTIME_SERVERS_PER_TAG + i);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE).size(),
          NUM_INSTANCES - NUM_SERVER_TAGS * NUM_SERVERS_PER_TAG - i * 2);
    }

    // Delete server tenants
    for (int i = 1; i < NUM_SERVER_TAGS; i++) {
      String serverTenant = SERVER_TAG_PREFIX + i;
      sendDeleteRequest(_controllerRequestURLBuilder.forServerTenantDelete(serverTenant));
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getOfflineTagForTenant(serverTenant))
          .size(), 0);
      Assert.assertEquals(_helixAdmin
          .getInstancesInClusterWithTag(getHelixClusterName(), TagNameUtils.getRealtimeTagForTenant(serverTenant))
          .size(), 0);
      Assert.assertEquals(_helixAdmin
              .getInstancesInClusterWithTag(getHelixClusterName(), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE).size(),
          NUM_INSTANCES - (NUM_SERVER_TAGS - i) * NUM_SERVERS_PER_TAG);
    }
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
