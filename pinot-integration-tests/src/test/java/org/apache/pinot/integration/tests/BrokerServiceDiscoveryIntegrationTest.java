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

import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test that starts one broker with auto-discovered echo service and test it
 */
public class BrokerServiceDiscoveryIntegrationTest extends ClusterTest {
  private static final String TENANT_NAME = "TestTenant";

  @Override
  public PinotConfiguration getDefaultBrokerConfiguration() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.BROKER_SERVICE_AUTO_DISCOVERY, true);
    return config;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    setUpTestDirectories(this.getClass().getSimpleName());
    TestUtils.ensureDirectoriesExistAndEmpty(getTempDir(), getSegmentDir(), getTarDir());

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);
  }

  @AfterClass
  public void tearDown()
          throws Exception {

    // Brokers and servers has been stopped
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(getTempDir());
  }

  @Test
  public void testBrokerExtraEndpointsAutoLoaded()
      throws Exception {
    String response = sendGetRequest(_brokerBaseApiUrl + "/test/echo/doge");
    Assert.assertEquals(response, "doge");
  }

  private static class BrokerServiceDiscoveryIntegrationDataSet
      extends DefaultIntegrationTestDataSet {

    public BrokerServiceDiscoveryIntegrationDataSet(ClusterTest clusterTest) {
      super(clusterTest);
    }

    @Override
    public String getBrokerTenant() {
      return TENANT_NAME;
    }

    @Override
    public String getServerTenant() {
      return TENANT_NAME;
    }
  }
}
