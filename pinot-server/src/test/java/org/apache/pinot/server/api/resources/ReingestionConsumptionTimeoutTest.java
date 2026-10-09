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
package org.apache.pinot.server.api.resources;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS;
import static org.assertj.core.api.Assertions.assertThat;


/// Tests that [ReingestionConsumptionTimeout] resolves the timeout from the cluster config, the server config and the
/// default, and follows cluster config changes.
public class ReingestionConsumptionTimeoutTest {
  private static final PinotConfiguration EMPTY_SERVER_CONF = new PinotConfiguration();
  private static final PinotConfiguration SERVER_CONF =
      new PinotConfiguration(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "3600000"));
  private static final long DEFAULT_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();

  @Test
  public void testDefaultsTo30Minutes() {
    assertThat(createTimeout(EMPTY_SERVER_CONF, Map.of()).getTimeoutMs()).isEqualTo(DEFAULT_TIMEOUT_MS);
  }

  @Test
  public void testServerConfig() {
    assertThat(createTimeout(SERVER_CONF, Map.of()).getTimeoutMs()).isEqualTo(3_600_000L);
  }

  @Test
  public void testClusterConfigOverridesServerConfig() {
    Map<String, String> clusterConfigs = Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "7200000");
    assertThat(createTimeout(SERVER_CONF, clusterConfigs).getTimeoutMs()).isEqualTo(7_200_000L);
    assertThat(createTimeout(EMPTY_SERVER_CONF, clusterConfigs).getTimeoutMs()).isEqualTo(7_200_000L);
  }

  @DataProvider
  public Object[][] validTimeouts() {
    return new Object[][]{{"1", 1L}, {" 7200000 ", 7_200_000L}, {String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE}};
  }

  @Test(dataProvider = "validTimeouts")
  public void testValidTimeouts(String timeout, long expectedTimeoutMs) {
    Map<String, String> configs = Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, timeout);
    assertThat(createTimeout(new PinotConfiguration(configs), Map.of()).getTimeoutMs()).isEqualTo(expectedTimeoutMs);
    assertThat(createTimeout(SERVER_CONF, configs).getTimeoutMs()).isEqualTo(expectedTimeoutMs);
  }

  @DataProvider
  public Object[][] invalidTimeouts() {
    return new Object[][]{{"abc"}, {""}, {"0"}, {"-1"}, {"1.5"}, {"30m"}, {"9223372036854775808"}};
  }

  @Test(dataProvider = "invalidTimeouts")
  public void testInvalidClusterConfigFallsBackToServerConfig(String invalidTimeout) {
    Map<String, String> clusterConfigs = Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, invalidTimeout);
    assertThat(createTimeout(SERVER_CONF, clusterConfigs).getTimeoutMs()).isEqualTo(3_600_000L);
  }

  @Test(dataProvider = "invalidTimeouts")
  public void testInvalidServerConfigFallsBackToDefault(String invalidTimeout) {
    PinotConfiguration serverConf =
        new PinotConfiguration(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, invalidTimeout));
    assertThat(createTimeout(serverConf, Map.of()).getTimeoutMs()).isEqualTo(DEFAULT_TIMEOUT_MS);
  }

  @Test
  public void testClusterConfigChangeIsPickedUpWithoutRestart() {
    // Registered before the cluster configs are loaded, as on server startup
    ReingestionConsumptionTimeout timeout = new ReingestionConsumptionTimeout(SERVER_CONF);
    DefaultClusterConfigChangeHandler clusterConfigProvider = new DefaultClusterConfigChangeHandler();
    clusterConfigProvider.registerClusterConfigChangeListener(timeout);
    assertThat(timeout.getTimeoutMs()).isEqualTo(3_600_000L);

    setClusterConfigs(clusterConfigProvider, Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "10800000"));
    assertThat(timeout.getTimeoutMs()).isEqualTo(10_800_000L);

    // An invalid cluster config falls back to the server config
    setClusterConfigs(clusterConfigProvider, Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "30m"));
    assertThat(timeout.getTimeoutMs()).isEqualTo(3_600_000L);

    setClusterConfigs(clusterConfigProvider, Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "14400000"));
    assertThat(timeout.getTimeoutMs()).isEqualTo(14_400_000L);

    // Removing the cluster config falls back to the server config
    setClusterConfigs(clusterConfigProvider, Map.of());
    assertThat(timeout.getTimeoutMs()).isEqualTo(3_600_000L);
  }

  /// Creates the timeout and registers it to a cluster config provider holding the given cluster configs.
  private static ReingestionConsumptionTimeout createTimeout(PinotConfiguration serverConf,
      Map<String, String> clusterConfigs) {
    DefaultClusterConfigChangeHandler clusterConfigProvider = new DefaultClusterConfigChangeHandler();
    setClusterConfigs(clusterConfigProvider, clusterConfigs);
    ReingestionConsumptionTimeout timeout = new ReingestionConsumptionTimeout(serverConf);
    clusterConfigProvider.registerClusterConfigChangeListener(timeout);
    return timeout;
  }

  private static void setClusterConfigs(DefaultClusterConfigChangeHandler clusterConfigProvider,
      Map<String, String> clusterConfigs) {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.getRecord().setSimpleFields(new HashMap<>(clusterConfigs));
    clusterConfigProvider.onClusterConfigChange(clusterConfig, null);
  }
}
