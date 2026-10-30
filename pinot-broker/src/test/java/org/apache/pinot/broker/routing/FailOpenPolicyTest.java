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
package org.apache.pinot.broker.routing;

import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FailOpenPolicyTest {

  @DataProvider
  public Object[][] brokerValues() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "brokerValues")
  public void testStartupClusterOverride(boolean brokerEnabled) {
    FailOpenPolicy policy = new FailOpenPolicy(brokerEnabled);
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.getRecord().setSimpleField(Broker.CONFIG_OF_ENABLE_FAIL_OPEN, Boolean.toString(!brokerEnabled));
    handler.onClusterConfigChange(clusterConfig, null);

    handler.registerClusterConfigChangeListener(policy);
    assertEquals(policy.isEnabled(), !brokerEnabled);
  }

  @Test(dataProvider = "brokerValues")
  public void testAbsentClusterValuePreservesBrokerValue(boolean brokerEnabled) {
    FailOpenPolicy policy = new FailOpenPolicy(brokerEnabled);
    new DefaultClusterConfigChangeHandler().registerClusterConfigChangeListener(policy);
    assertEquals(policy.isEnabled(), brokerEnabled);
  }

  @Test
  public void testDynamicEnableAndDisable() {
    FailOpenPolicy policy = new FailOpenPolicy(false);
    String key = Broker.CONFIG_OF_ENABLE_FAIL_OPEN;

    policy.onChange(Set.of(key), Map.of(key, Boolean.TRUE.toString()));
    assertTrue(policy.isEnabled());

    policy.onChange(Set.of(key), Map.of(key, Boolean.FALSE.toString()));
    assertFalse(policy.isEnabled());
  }

  @Test
  public void testDeletionRestoresBrokerValue() {
    FailOpenPolicy policy = new FailOpenPolicy(true);
    policy.onChange(Set.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN), Map.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN, "false"));
    assertFalse(policy.isEnabled());
    policy.onChange(Set.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN), Map.of());
    assertTrue(policy.isEnabled());
  }

  @Test
  public void testDeletionRestoresDefault() {
    FailOpenPolicy policy = new FailOpenPolicy(Broker.DEFAULT_ENABLE_FAIL_OPEN);
    policy.onChange(Set.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN), Map.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN, "true"));
    assertTrue(policy.isEnabled());
    policy.onChange(Set.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN), Map.of());
    assertFalse(policy.isEnabled());
  }

  @Test
  public void testUnrelatedChangeIsIgnored() {
    FailOpenPolicy policy = new FailOpenPolicy(false);
    policy.onChange(Set.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN), Map.of(Broker.CONFIG_OF_ENABLE_FAIL_OPEN, "true"));
    policy.onChange(Set.of("unrelated.config"), Map.of());
    assertTrue(policy.isEnabled());
  }
}
