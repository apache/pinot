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
package org.apache.pinot.broker.requesthandler;

import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ApproximateFunctionOverrideProviderTest {

  @Test
  public void testBrokerConfProvidesTheDefault() {
    ApproximateFunctionOverrideProvider unset = newProvider(Map.of());
    assertFalse(unset.getSettings().isEnabled(null, null));
    assertEquals(unset.getSettings()._distinctCountParams, "");
    assertEquals(unset.getSettings()._percentileParams, "");

    ApproximateFunctionOverrideProvider provider = newProvider(Map.of(
        Broker.USE_APPROXIMATE_FUNCTION, "true",
        Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, "threshold=10;log2m=8",
        Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=20;compression=50"));
    assertTrue(provider.getSettings().isEnabled(null, null));
    assertEquals(provider.getSettings()._distinctCountParams, "threshold=10;log2m=8");
    assertEquals(provider.getSettings()._percentileParams, "threshold=20;compression=50");
  }

  @Test
  public void testPrecedenceIsQueryOptionThenTableThenDefault() {
    ApproximateFunctionOverrideProvider disabledByDefault = newProvider(Map.of());
    assertTrue(disabledByDefault.getSettings().isEnabled(Boolean.TRUE, null));
    assertTrue(disabledByDefault.getSettings().isEnabled(null, Boolean.TRUE));
    // The query option outranks the table config in both directions.
    assertFalse(disabledByDefault.getSettings().isEnabled(Boolean.FALSE, Boolean.TRUE));

    ApproximateFunctionOverrideProvider enabledByDefault =
        newProvider(Map.of(Broker.USE_APPROXIMATE_FUNCTION, "true"));
    assertFalse(enabledByDefault.getSettings().isEnabled(Boolean.FALSE, null));
    assertFalse(enabledByDefault.getSettings().isEnabled(null, Boolean.FALSE));
    assertTrue(enabledByDefault.getSettings().isEnabled(Boolean.TRUE, Boolean.FALSE));
  }

  @Test
  public void testClusterConfigWinsOverBrokerConfAndReloads() {
    ApproximateFunctionOverrideProvider provider = newProvider(Map.of(
        Broker.USE_APPROXIMATE_FUNCTION, "false",
        Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=20"));

    Map<String, String> clusterConfigs = Map.of(
        Broker.USE_APPROXIMATE_FUNCTION, "true",
        Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=5;compression=200");
    provider.onChange(clusterConfigs.keySet(), clusterConfigs);

    assertTrue(provider.getSettings().isEnabled(null, null));
    assertEquals(provider.getSettings()._percentileParams, "threshold=5;compression=200");

    // Removing the keys from the cluster config falls back to the broker conf.
    provider.onChange(clusterConfigs.keySet(), Map.of());
    assertFalse(provider.getSettings().isEnabled(null, null));
    assertEquals(provider.getSettings()._percentileParams, "threshold=20");
  }

  @Test
  public void testUnrelatedClusterConfigChangeIsIgnored() {
    ApproximateFunctionOverrideProvider provider =
        newProvider(Map.of(Broker.USE_APPROXIMATE_FUNCTION, "true"));
    provider.onChange(Map.of("some.other.key", "1").keySet(), Map.of("some.other.key", "1"));
    assertTrue(provider.getSettings().isEnabled(null, null));
  }

  /// A typo in the cluster config would otherwise fail every rewritten query on the servers, so a bad value is
  /// rejected in favour of the broker conf value. Falling back to the broker conf rather than to the last good live
  /// value is what makes a broker that restarts resolve the same value as one that stayed up.
  @Test
  public void testInvalidParamsAreRejectedInFavourOfTheBrokerConfValue() {
    ApproximateFunctionOverrideProvider provider = newProvider(Map.of(
        Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, "threshold=10",
        Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=10"));

    onChange(provider, Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, "thresold=10");
    assertEquals(provider.getSettings()._distinctCountParams, "threshold=10");

    onChange(provider, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=abc");
    assertEquals(provider.getSettings()._percentileParams, "threshold=10");

    // The percentile parameters must not be accepted for distinct count, and the other way round.
    onChange(provider, Broker.APPROXIMATE_FUNCTION_DISTINCT_COUNT_PARAMS, "compression=50");
    assertEquals(provider.getSettings()._distinctCountParams, "threshold=10");

    // A good live value does not become the fallback for a later bad one.
    onChange(provider, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=500");
    assertEquals(provider.getSettings()._percentileParams, "threshold=500");
    onChange(provider, Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=nope");
    assertEquals(provider.getSettings()._percentileParams, "threshold=10");
  }

  /// A broker conf that is invalid at startup must not leave the broker emitting calls the servers reject.
  @Test
  public void testInvalidBrokerConfParamsFallBackToTheFunctionDefaults() {
    ApproximateFunctionOverrideProvider provider =
        newProvider(Map.of(Broker.APPROXIMATE_FUNCTION_PERCENTILE_PARAMS, "threshold=10;log2m=8"));
    assertEquals(provider.getSettings()._percentileParams, "");
  }

  private static void onChange(ApproximateFunctionOverrideProvider provider, String key, String value) {
    Map<String, String> clusterConfigs = Map.of(key, value);
    provider.onChange(clusterConfigs.keySet(), clusterConfigs);
  }

  private static ApproximateFunctionOverrideProvider newProvider(Map<String, Object> properties) {
    return new ApproximateFunctionOverrideProvider(new PinotConfiguration(properties));
  }
}
