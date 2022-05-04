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
public class ControllerServiceDiscoveryIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String TENANT_NAME = "TestTenant";

  @Override
  protected String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected String getServerTenant() {
    return TENANT_NAME;
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> retVal = super.getDefaultControllerConfiguration();
    retVal.put(CommonConstants.Controller.CONTROLLER_SERVICE_AUTO_DISCOVERY, true);
    return retVal;
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Broker.BROKER_SERVICE_AUTO_DISCOVERY, true);
    return config;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

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
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testControllerExtraEndpointsAutoLoaded()
      throws Exception {
    String response = sendGetRequest(_controllerBaseApiUrl + "/test/echo/doge");
    Assert.assertEquals(response, "doge");
    response = sendGetRequest(_brokerBaseApiUrl + "/test/echo/doge");
    Assert.assertEquals(response, "doge");
  }
}
