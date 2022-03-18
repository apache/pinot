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

import java.util.Collections;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


/**
 * Integration test that extends OfflineClusterIntegrationTest but start multiple brokers and servers.
 */
public class MultiNodesOfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  private static final int NUM_BROKERS = 2;
  private static final int NUM_SERVERS = 3;

  @Override
  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected void startServers() {
    startServers(NUM_SERVERS);
  }

  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    // Add a new broker to the cluster
    Map<String, Object> properties = getDefaultBrokerConfiguration().toMap();
    String clusterName = getHelixClusterName();
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    int port = NetUtils.findOpenPort(DEFAULT_BROKER_PORT);
    properties.put(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    properties.put(CommonConstants.Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    // Check if broker is added to all the tables in broker resource
    String brokerId = brokerStarter.getInstanceId();
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertEquals(brokerAssignment.get(brokerId), BrokerResourceStateModel.ONLINE);
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (!brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to find broker in broker resource ExternalView");

    // Stop the broker
    brokerStarter.stop();

    // Dropping the broker should fail because it is still in the broker resource
    try {
      sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));
      fail("Dropping instance should fail because it is still in the broker resource");
    } catch (Exception e) {
      // Expected
    }

    // Untag the broker and update the broker resource so that it is removed from the broker resource
    sendPutRequest(_controllerRequestURLBuilder.forInstanceUpdateTags(brokerId, Collections.emptyList(), true));

    // Check if broker is removed from all the tables in broker resource
    brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertFalse(brokerAssignment.containsKey(brokerId));
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove broker from broker resource ExternalView");

    // Dropping the broker should success now
    sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));

    // Check if broker is dropped from the cluster
    assertFalse(_helixAdmin.getInstancesInCluster(clusterName).contains(brokerId));
  }

  @Test(enabled = false)
  @Override
  public void testHardcodedServerPartitionedSqlQueries() {
    // Ignored
  }
}
