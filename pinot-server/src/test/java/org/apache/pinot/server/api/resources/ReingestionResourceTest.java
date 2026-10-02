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

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.server.api.BaseResourceTest;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.server.api.resources.ReingestionResource.getConsumptionTimeoutMs;
import static org.apache.pinot.server.api.resources.ReingestionResource.waitForCondition;
import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/// Tests the consumption timeout resolution of [ReingestionResource] and the wiring of its injected dependencies.
public class ReingestionResourceTest extends BaseResourceTest {
  private static final PinotConfiguration EMPTY_SERVER_CONF = new PinotConfiguration();
  private static final PinotConfiguration SERVER_CONF =
      new PinotConfiguration(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "3600000"));

  @Test
  public void testResourceDependenciesAreInjected() {
    // The resource is created per request, so any request fails if one of its injected dependencies is not bound
    Response response = _webTarget.path("/reingestSegment/jobs").request().get(Response.class);
    assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
  }

  @Test
  public void testConsumptionTimeoutDefaultsTo30Minutes() {
    assertThat(DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS).isEqualTo(30 * 60 * 1000L);
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider(Map.of()), EMPTY_SERVER_CONF)).isEqualTo(
        DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS);
  }

  @Test
  public void testConsumptionTimeoutFromServerConfig() {
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider(Map.of()), SERVER_CONF)).isEqualTo(3_600_000L);
  }

  @Test
  public void testClusterConfigOverridesServerConfig() {
    DefaultClusterConfigChangeHandler clusterConfigProvider =
        clusterConfigProvider(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "7200000"));
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, SERVER_CONF)).isEqualTo(7_200_000L);
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, EMPTY_SERVER_CONF)).isEqualTo(7_200_000L);
  }

  @DataProvider
  public Object[][] validTimeouts() {
    return new Object[][]{{"1", 1L}, {" 7200000 ", 7_200_000L}, {String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE}};
  }

  @Test(dataProvider = "validTimeouts")
  public void testValidTimeouts(String timeout, long expectedTimeoutMs) {
    PinotConfiguration serverConf =
        new PinotConfiguration(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, timeout));
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider(Map.of()), serverConf)).isEqualTo(expectedTimeoutMs);
    assertThat(getConsumptionTimeoutMs(
        clusterConfigProvider(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, timeout)), SERVER_CONF)).isEqualTo(
        expectedTimeoutMs);
  }

  @DataProvider
  public Object[][] invalidTimeouts() {
    return new Object[][]{{"abc"}, {""}, {"0"}, {"-1"}, {"1.5"}, {"30m"}, {"9223372036854775808"}};
  }

  @Test(dataProvider = "invalidTimeouts")
  public void testInvalidClusterConfigFallsBackToServerConfig(String invalidTimeout) {
    DefaultClusterConfigChangeHandler clusterConfigProvider =
        clusterConfigProvider(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, invalidTimeout));
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, SERVER_CONF)).isEqualTo(3_600_000L);
  }

  @Test(dataProvider = "invalidTimeouts")
  public void testInvalidServerConfigFallsBackToDefault(String invalidTimeout) {
    PinotConfiguration serverConf =
        new PinotConfiguration(Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, invalidTimeout));
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider(Map.of()), serverConf)).isEqualTo(
        DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS);
  }

  @Test
  public void testClusterConfigChangeIsPickedUpWithoutRestart() {
    DefaultClusterConfigChangeHandler clusterConfigProvider = clusterConfigProvider(Map.of());
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, SERVER_CONF)).isEqualTo(3_600_000L);

    // Set the cluster config on the same provider
    setClusterConfigs(clusterConfigProvider, Map.of(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, "10800000"));
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, SERVER_CONF)).isEqualTo(10_800_000L);

    // Remove the cluster config, which falls back to the server config
    setClusterConfigs(clusterConfigProvider, Map.of());
    assertThat(getConsumptionTimeoutMs(clusterConfigProvider, SERVER_CONF)).isEqualTo(3_600_000L);
  }

  @Test
  public void testWaitForConditionWithMaxTimeoutDoesNotOverflow() {
    // Without saturation, the deadline overflows to the past and the wait times out before checking the condition
    waitForCondition(v -> true, 1, Long.MAX_VALUE, 0);
    assertThatThrownBy(() -> waitForCondition(v -> false, 1, 10, 0)).hasMessageContaining("Timeout");
  }

  private static DefaultClusterConfigChangeHandler clusterConfigProvider(Map<String, String> clusterConfigs) {
    DefaultClusterConfigChangeHandler clusterConfigProvider = new DefaultClusterConfigChangeHandler();
    setClusterConfigs(clusterConfigProvider, clusterConfigs);
    return clusterConfigProvider;
  }

  private static void setClusterConfigs(DefaultClusterConfigChangeHandler clusterConfigProvider,
      Map<String, String> clusterConfigs) {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.getRecord().setSimpleFields(new HashMap<>(clusterConfigs));
    clusterConfigProvider.onClusterConfigChange(clusterConfig, null);
  }
}
