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
package org.apache.pinot.common.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class DefaultClusterConfigChangeHandlerTest {
  private static final String PINNED_KEY = "pinot.test.pinned";
  private static final String INSTANCE_ONLY_KEY = "pinot.test.instanceOnly";
  private static final String CLUSTER_ONLY_KEY = "pinot.test.clusterOnly";

  @Test
  public void testDefaultConstructorStartsWithEmptyConfigs() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    RecordingListener listener = new RecordingListener();

    handler.registerClusterConfigChangeListener(listener);

    assertEquals(listener.getLastChangedConfigs(), Set.of());
    assertEquals(listener.getLastConfigs(), Map.of());
  }

  @Test
  public void testNullInstanceValuesAreIgnored() {
    PinotConfiguration instanceConfig = mock(PinotConfiguration.class);
    when(instanceConfig.getKeys()).thenReturn(List.of(PINNED_KEY, INSTANCE_ONLY_KEY));
    when(instanceConfig.getProperty(PINNED_KEY)).thenReturn("instance-value");
    when(instanceConfig.getProperty(INSTANCE_ONLY_KEY)).thenReturn(null);

    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler(instanceConfig);

    assertConfigs(handler.getClusterConfigs(), Map.of(PINNED_KEY, "instance-value"));

    Map<String, String> configsWithNull = new HashMap<>();
    configsWithNull.put(PINNED_KEY, "instance-value");
    configsWithNull.put(INSTANCE_ONLY_KEY, null);
    handler = new DefaultClusterConfigChangeHandler(configsWithNull);

    assertConfigs(handler.getClusterConfigs(), Map.of(PINNED_KEY, "instance-value"));
  }

  @Test
  public void testInstanceConfigsTakePrecedenceOverClusterConfigs() {
    Map<String, String> instanceConfigs = new HashMap<>();
    instanceConfigs.put(PINNED_KEY.toLowerCase(), "instance-value");
    instanceConfigs.put(INSTANCE_ONLY_KEY.toLowerCase(), "instance-only-value");
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler(instanceConfigs);
    RecordingListener listener = new RecordingListener();
    handler.registerClusterConfigChangeListener(listener);

    assertEquals(listener.getLastChangedConfigs(), Set.of(PINNED_KEY, INSTANCE_ONLY_KEY));
    assertConfigs(listener.getLastConfigs(), Map.of(
        PINNED_KEY, "instance-value",
        INSTANCE_ONLY_KEY, "instance-only-value"));

    // Mutating the source map after construction must not change the captured instance configuration.
    instanceConfigs.put(PINNED_KEY.toLowerCase(), "mutated-value");

    listener.clear();
    handler.onClusterConfigChange(clusterConfig(Map.of(
        PINNED_KEY, "cluster-value",
        CLUSTER_ONLY_KEY, "cluster-value-1")), null);

    assertEquals(listener.getLastChangedConfigs(), Set.of(CLUSTER_ONLY_KEY));
    assertConfigs(listener.getLastConfigs(), Map.of(
        PINNED_KEY, "instance-value",
        INSTANCE_ONLY_KEY, "instance-only-value",
        CLUSTER_ONLY_KEY, "cluster-value-1"));
    assertEquals(handler.getClusterConfigs(), listener.getLastConfigs());

    listener.clear();
    handler.onClusterConfigChange(clusterConfig(Map.of(
        PINNED_KEY, "updated-cluster-value",
        CLUSTER_ONLY_KEY, "cluster-value-2")), null);

    assertEquals(listener.getLastChangedConfigs(), Set.of(CLUSTER_ONLY_KEY));
    assertEquals(listener.getLastConfigs().get(PINNED_KEY), "instance-value");
    assertEquals(listener.getLastConfigs().get(CLUSTER_ONLY_KEY), "cluster-value-2");

    listener.clear();
    handler.onClusterConfigChange(clusterConfig(Map.of()), null);

    assertEquals(listener.getLastChangedConfigs(), Set.of(CLUSTER_ONLY_KEY));
    assertEquals(listener.getLastConfigs().get(PINNED_KEY), "instance-value");
    assertConfigs(listener.getLastConfigs(), Map.of(
        PINNED_KEY, "instance-value",
        INSTANCE_ONLY_KEY, "instance-only-value"));
  }

  @Test
  public void testLateRegistrationReceivesEffectiveConfigs() {
    DefaultClusterConfigChangeHandler handler =
        new DefaultClusterConfigChangeHandler(Map.of(PINNED_KEY, "instance-value"));
    handler.onClusterConfigChange(clusterConfig(Map.of(
        PINNED_KEY, "cluster-value",
        CLUSTER_ONLY_KEY, "cluster-only-value")), null);

    RecordingListener listener = new RecordingListener();
    handler.registerClusterConfigChangeListener(listener);

    assertEquals(listener.getLastChangedConfigs(), Set.of(PINNED_KEY, CLUSTER_ONLY_KEY));
    assertConfigs(listener.getLastConfigs(), Map.of(
        PINNED_KEY, "instance-value",
        CLUSTER_ONLY_KEY, "cluster-only-value"));
  }

  private static void assertConfigs(Map<String, String> actual, Map<String, String> expected) {
    assertEquals(actual.size(), expected.size());
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      assertEquals(actual.get(entry.getKey()), entry.getValue());
    }
  }

  private static ClusterConfig clusterConfig(Map<String, String> configs) {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.getRecord().setSimpleFields(configs);
    return clusterConfig;
  }

  private static class RecordingListener implements PinotClusterConfigChangeListener {
    private final List<Set<String>> _changedConfigs = new ArrayList<>();
    private final List<Map<String, String>> _configs = new ArrayList<>();

    @Override
    public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      _changedConfigs.add(changedConfigs);
      _configs.add(clusterConfigs);
    }

    private Set<String> getLastChangedConfigs() {
      return _changedConfigs.get(_changedConfigs.size() - 1);
    }

    private Map<String, String> getLastConfigs() {
      return _configs.get(_configs.size() - 1);
    }

    private void clear() {
      _changedConfigs.clear();
      _configs.clear();
    }
  }
}
