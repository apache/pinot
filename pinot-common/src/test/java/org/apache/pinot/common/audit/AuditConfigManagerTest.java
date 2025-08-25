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
package org.apache.pinot.common.audit;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for AuditConfigManager to verify correct config injection
 * when onClusterConfigChange is called.
 */
public class AuditConfigManagerTest {

  @Test
  public void testOnClusterConfigChangeWithAllConfigs() {
    // Given
    ClusterConfig clusterConfig = createClusterConfig(
        Map.of("pinot.audit.enabled", "true", "pinot.audit.capture.request.payload.enabled", "true",
            "pinot.audit.capture.request.headers", "true", "pinot.audit.payload.size.max.bytes", "20480",
            "pinot.audit.excluded.endpoints", "/health,/metrics"));

    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onClusterConfigChange(clusterConfig, null);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.isCaptureRequestPayload()).isTrue();
    assertThat(config.isCaptureRequestHeaders()).isTrue();
    assertThat(config.getMaxPayloadSize()).isEqualTo(20480);
    assertThat(config.getExcludedEndpoints()).isEqualTo("/health,/metrics");
  }

  @Test
  public void testOnClusterConfigChangeWithPartialConfigs() {
    // Given
    ClusterConfig clusterConfig =
        createClusterConfig(Map.of("pinot.audit.enabled", "true", "pinot.audit.payload.size.max.bytes", "5000"));

    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onClusterConfigChange(clusterConfig, null);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getMaxPayloadSize()).isEqualTo(5000);
    // Verify defaults for unspecified configs
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.isCaptureRequestHeaders()).isFalse();
    assertThat(config.getExcludedEndpoints()).isEmpty();
  }

  @Test
  public void testOnClusterConfigChangeWithNoAuditConfigs() {
    // Given
    ClusterConfig clusterConfig = createClusterConfig(Map.of());
    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onClusterConfigChange(clusterConfig, null);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isFalse();
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.isCaptureRequestHeaders()).isFalse();
    assertThat(config.getMaxPayloadSize()).isEqualTo(10240);
    assertThat(config.getExcludedEndpoints()).isEmpty();
  }

  @Test
  public void testConfigUpdateOverwritesPrevious() {
    // Given
    AuditConfigManager manager = new AuditConfigManager();

    // Set initial config
    ClusterConfig initialConfig =
        createClusterConfig(Map.of("pinot.audit.enabled", "true", "pinot.audit.payload.size.max.bytes", "15000"));
    manager.onClusterConfigChange(initialConfig, null);
    assertThat(manager.getCurrentConfig().isEnabled()).isTrue();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(15000);

    // When - Update with new config
    ClusterConfig updatedConfig =
        createClusterConfig(Map.of("pinot.audit.enabled", "false", "pinot.audit.payload.size.max.bytes", "25000"));
    manager.onClusterConfigChange(updatedConfig, null);

    // Then
    assertThat(manager.getCurrentConfig().isEnabled()).isFalse();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(25000);
  }

  @Test
  public void testBuildFromClusterConfigDirectly() {
    // Given
    ClusterConfig clusterConfig = createClusterConfig(
        Map.of("pinot.audit.enabled", "true", "pinot.audit.capture.request.payload.enabled", "false",
            "pinot.audit.capture.request.headers", "true"));

    // When
    AuditConfig config = AuditConfigManager.buildFromClusterConfig(clusterConfig);

    // Then
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.isCaptureRequestHeaders()).isTrue();
    // Verify defaults for unspecified fields
    assertThat(config.getMaxPayloadSize()).isEqualTo(10240);
    assertThat(config.getExcludedEndpoints()).isEmpty();
  }

  // Helper method to create ClusterConfig with given properties
  private ClusterConfig createClusterConfig(Map<String, String> properties) {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    Map<String, String> allProperties = new HashMap<>(properties);
    // Add some non-audit configs to verify filtering works
    allProperties.put("some.other.config", "value");
    allProperties.put("another.config", "123");
    clusterConfig.getRecord().setSimpleFields(allProperties);
    return clusterConfig;
  }
}
