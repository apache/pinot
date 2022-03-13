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
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.spi.utils.NetUtils;
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
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    int port = NetUtils.findOpenPort(DEFAULT_BROKER_PORT);
    properties.put(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    properties.put(CommonConstants.Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    // Check if broker is added to all the tables in broker resource
    String brokerId = brokerStarter.getInstanceId();
    IdealState brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
    for (Map<String, String> brokerAssignment : brokerResource.getRecord().getMapFields().values()) {
      assertEquals(brokerAssignment.get(brokerId), BrokerResourceStateModel.ONLINE);
    }

    // Stop and drop the broker
    brokerStarter.stop();
    try {
      sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));
      fail("Dropping instance should fail because it is still in the broker resource");
    } catch (Exception e) {
      // Expected
    }
    // Untag the broker and update the broker resource so that it is removed from the broker resource
    sendPutRequest(_controllerRequestURLBuilder.forInstanceUpdateTags(brokerId, Collections.emptyList(), true));
    // Check if broker is removed from all the tables in broker resource
    brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
    for (Map<String, String> brokerAssignment : brokerResource.getRecord().getMapFields().values()) {
      assertFalse(brokerAssignment.containsKey(brokerId));
    }
    // Dropping instance should success
    sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));
    // Check if broker is dropped from the cluster
    assertFalse(_helixAdmin.getInstancesInCluster(getHelixClusterName()).contains(brokerId));
  }

  @Test(enabled = false)
  @Override
  public void testHardcodedServerPartitionedSqlQueries() {
    // Ignored
  }
}
