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
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// Verifies fail-open cluster overrides through a running broker and its Helix listener.
/// This test owns one embedded cluster and is not thread-safe.
public class BrokerFailOpenPolicyIntegrationTest extends BaseClusterIntegrationTest {
  private static final String CONFIG_KEY = Broker.CONFIG_OF_ENABLE_FAIL_OPEN;

  private TestBrokerStarter _brokerStarter;

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CONFIG_KEY, false);
  }

  @Override
  protected BaseBrokerStarter createBrokerStarter() {
    _brokerStarter = new TestBrokerStarter();
    return _brokerStarter;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    updateClusterConfig(Map.of(CONFIG_KEY, "true"));
    startBroker();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testClusterOverrideAndUpdates()
      throws Exception {
    assertTrue(_brokerStarter.isFailOpenEnabled(), "Cluster config should override broker config at startup");

    updateClusterConfig(Map.of(CONFIG_KEY, "false"));
    waitForFailOpenEnabled(false);
    updateClusterConfig(Map.of(CONFIG_KEY, "true"));
    waitForFailOpenEnabled(true);

    deleteClusterConfig(CONFIG_KEY);
    waitForFailOpenEnabled(false);
  }

  private void waitForFailOpenEnabled(boolean expected) {
    TestUtils.waitForCondition(aVoid -> _brokerStarter.isFailOpenEnabled() == expected, 100L, 10_000L,
        "Fail-open policy did not update to: " + expected);
  }

  /// Exposes the effective policy state without adding a production accessor.
  private static class TestBrokerStarter extends HelixBrokerStarter {
    boolean isFailOpenEnabled() {
      return _failOpenPolicy.isEnabled();
    }
  }
}
