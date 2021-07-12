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
package org.apache.pinot.broker.broker;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_BROKER_HOSTNAME;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.Instance.INSTANCE_ID_KEY;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT;
import static org.testng.Assert.assertEquals;


public class HelixBrokerStarterHostnamePortTest extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  @Test
  public void testHostnamePortOverride()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    properties.put(CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(INSTANCE_ID_KEY, "Broker_myInstance");
    properties.put(CONFIG_OF_BROKER_HOSTNAME, "myHost");
    properties.put(KEY_OF_BROKER_QUERY_PORT, 1234);

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    String instanceId = brokerStarter.getInstanceId();
    assertEquals(instanceId, "Broker_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");

    brokerStarter.stop();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInvalidInstanceId()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    properties.put(CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(INSTANCE_ID_KEY, "myInstance");
    properties.put(CONFIG_OF_BROKER_HOSTNAME, "myHost");
    properties.put(KEY_OF_BROKER_QUERY_PORT, 1234);

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
  }

  @Test
  public void testDefaultInstanceId()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    properties.put(CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(CONFIG_OF_BROKER_HOSTNAME, "myHost");
    properties.put(KEY_OF_BROKER_QUERY_PORT, 1234);

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    String instanceId = brokerStarter.getInstanceId();
    assertEquals(instanceId, "Broker_myHost_1234");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");

    brokerStarter.stop();
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
