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
import java.util.Set;
import org.apache.pinot.spi.services.ServiceRole;
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
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.audit.controller.enabled", "true");
    properties.put("pinot.audit.controller.capture.request.payload.enabled", "true");
    properties.put("pinot.audit.controller.capture.request.headers", "Content-Type,X-Request-Id,Authorization");
    properties.put("pinot.audit.controller.request.payload.size.max.bytes", "20480");
    properties.put("pinot.audit.controller.url.filter.exclude.patterns", "/health,/metrics");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // When
    manager.onChange(properties.keySet(), properties);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.isCaptureRequestPayload()).isTrue();
    assertThat(config.getCaptureRequestHeaders()).isEqualTo("Content-Type,X-Request-Id,Authorization");
    assertThat(config.getMaxPayloadSize()).isEqualTo(20480);
    assertThat(config.getUrlFilterExcludePatterns()).isEqualTo("/health,/metrics");
  }

  @Test
  public void testOnClusterConfigChangeWithPartialConfigs() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.audit.controller.enabled", "true");
    properties.put("pinot.audit.controller.request.payload.size.max.bytes", "5000");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // When
    manager.onChange(properties.keySet(), properties);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getMaxPayloadSize()).isEqualTo(5000);
    // Verify defaults for unspecified configs
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.getCaptureRequestHeaders()).isEmpty();
    assertThat(config.getUrlFilterExcludePatterns()).isEmpty();
  }

  @Test
  public void testOnClusterConfigChangeWithNoAuditConfigs() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");
    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // When
    manager.onChange(properties.keySet(), properties);

    // Then
    AuditConfig config = manager.getCurrentConfig();
    assertThat(config.isEnabled()).isFalse();
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.getCaptureRequestHeaders()).isEmpty();
    assertThat(config.getMaxPayloadSize()).isEqualTo(AuditConfig.MAX_AUDIT_PAYLOAD_SIZE_BYTES_DEFAULT);
    assertThat(config.getUrlFilterExcludePatterns()).isEmpty();
  }

  @Test
  public void testConfigUpdateOverwritesPrevious() {
    // Given
    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // Set initial config
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.audit.controller.enabled", "true");
    initialProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "15000");
    manager.onChange(initialProperties.keySet(), initialProperties);
    assertThat(manager.getCurrentConfig().isEnabled()).isTrue();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(15000);

    // When - Update with new config
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put("pinot.audit.controller.enabled", "false");
    updatedProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "25000");
    manager.onChange(updatedProperties.keySet(), updatedProperties);

    // Then
    assertThat(manager.getCurrentConfig().isEnabled()).isFalse();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(25000);
  }

  @Test
  public void testBuildFromClusterConfigDirectly() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.audit.controller.enabled", "true");
    properties.put("pinot.audit.controller.capture.request.payload.enabled", "false");
    properties.put("pinot.audit.controller.capture.request.headers", "X-User-Id,X-Session-Token");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    // When
    AuditConfig config = AuditConfigManager.buildFromClusterConfig(properties, "pinot.audit.controller");

    // Then
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.getCaptureRequestHeaders()).isEqualTo("X-User-Id,X-Session-Token");
    // Verify defaults for unspecified fields
    assertThat(config.getMaxPayloadSize()).isEqualTo(AuditConfig.MAX_AUDIT_PAYLOAD_SIZE_BYTES_DEFAULT);
    assertThat(config.getUrlFilterExcludePatterns()).isEmpty();
  }

  @Test
  public void testOnChangeSkipsRebuildWhenNoAuditConfigsChanged() {
    // Given - AuditConfigManager with initial config
    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.audit.controller.enabled", "true");
    initialProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "15000");
    manager.onChange(initialProperties.keySet(), initialProperties);

    // Capture initial config instance - this should NOT change after non-audit config changes
    AuditConfig configBeforeNonAuditChange = manager.getCurrentConfig();
    assertThat(configBeforeNonAuditChange.isEnabled()).isTrue();
    assertThat(configBeforeNonAuditChange.getMaxPayloadSize()).isEqualTo(15000);

    // When - Update with only non-audit configs
    Map<String, String> nonAuditProperties = new HashMap<>();
    nonAuditProperties.put("some.other.config", "newValue");
    nonAuditProperties.put("another.config", "456");
    // Include the previous audit configs to simulate cluster state
    nonAuditProperties.putAll(initialProperties);

    manager.onChange(Set.of("some.other.config", "another.config"), nonAuditProperties);

    // Then - Config instance should be the exact same object (no rebuild occurred)
    AuditConfig configAfterNonAuditChange = manager.getCurrentConfig();
    assertThat(configAfterNonAuditChange).isSameAs(configBeforeNonAuditChange);
  }

  @Test
  public void testOnChangeRebuildsWhenAuditConfigsChanged() {
    // Given - AuditConfigManager with initial config
    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.audit.controller.enabled", "false");
    initialProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "10000");
    manager.onChange(initialProperties.keySet(), initialProperties);

    assertThat(manager.getCurrentConfig().isEnabled()).isFalse();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(10000);

    // When - Update with audit configs changed
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put("pinot.audit.controller.enabled", "true");
    updatedProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "20000");
    updatedProperties.put("some.other.config", "value");

    manager.onChange(Set.of("pinot.audit.controller.enabled", "pinot.audit.controller.request.payload.size.max.bytes"),
        updatedProperties);

    // Then - Config should be rebuilt with new audit config values
    AuditConfig updatedConfig = manager.getCurrentConfig();
    assertThat(updatedConfig.isEnabled()).isTrue();
    assertThat(updatedConfig.getMaxPayloadSize()).isEqualTo(20000);
  }

  @Test
  public void testZookeeperConfigDeletionRevertsToDefaults() {
    // Given
    AuditConfigManager manager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // Set initial custom configs
    Map<String, String> customProperties = new HashMap<>();
    customProperties.put("pinot.audit.controller.enabled", "true");
    customProperties.put("pinot.audit.controller.capture.request.payload.enabled", "true");
    customProperties.put("pinot.audit.controller.capture.request.headers", "X-Trace-Id,X-Correlation-Id");
    customProperties.put("pinot.audit.controller.request.payload.size.max.bytes", "50000");
    customProperties.put("pinot.audit.controller.url.filter.exclude.patterns", "/test,/debug");
    manager.onChange(customProperties.keySet(), customProperties);

    // Verify custom configs are applied
    AuditConfig customConfig = manager.getCurrentConfig();
    assertThat(customConfig.isEnabled()).isTrue();
    assertThat(customConfig.isCaptureRequestPayload()).isTrue();
    assertThat(customConfig.getCaptureRequestHeaders()).isEqualTo("X-Trace-Id,X-Correlation-Id");
    assertThat(customConfig.getMaxPayloadSize()).isEqualTo(50000);
    assertThat(customConfig.getUrlFilterExcludePatterns()).isEqualTo("/test,/debug");

    // When - Simulate ZooKeeper config deletion with empty map
    // The changedConfigs should contain the keys that were deleted, but clusterConfigs should be empty
    Map<String, String> emptyProperties = new HashMap<>();
    manager.onChange(customProperties.keySet(), emptyProperties);

    // Then - Verify all configs revert to defaults as defined in AuditConfig class
    AuditConfig defaultConfig = manager.getCurrentConfig();
    assertThat(defaultConfig.isEnabled()).isFalse();
    assertThat(defaultConfig.isCaptureRequestPayload()).isFalse();
    assertThat(defaultConfig.getCaptureRequestHeaders()).isEmpty();
    assertThat(defaultConfig.getMaxPayloadSize()).isEqualTo(AuditConfig.MAX_AUDIT_PAYLOAD_SIZE_BYTES_DEFAULT);
    assertThat(defaultConfig.getUrlFilterExcludePatterns()).isEmpty();
  }

  @Test
  public void testConfigurationStringFormats() {
    Map<String, String> properties = new HashMap<>();

    // Test various string formats
    properties.put("pinot.audit.controller.capture.request.headers", "Header1,Header2,Header3");
    AuditConfig config1 = AuditConfigManager.buildFromClusterConfig(properties, "pinot.audit.controller");
    assertThat(config1.getCaptureRequestHeaders()).isEqualTo("Header1,Header2,Header3");

    // Test empty string
    properties.put("pinot.audit.controller.capture.request.headers", "");
    AuditConfig config2 = AuditConfigManager.buildFromClusterConfig(properties, "pinot.audit.controller");
    assertThat(config2.getCaptureRequestHeaders()).isEmpty();

    // Test single header
    properties.put("pinot.audit.controller.capture.request.headers", "Content-Type");
    AuditConfig config3 = AuditConfigManager.buildFromClusterConfig(properties, "pinot.audit.controller");
    assertThat(config3.getCaptureRequestHeaders()).isEqualTo("Content-Type");

    // Test headers with mixed case and special characters
    properties.put("pinot.audit.controller.capture.request.headers",
        "Content-Type,X-Request-ID,User-Agent,X-Custom-123");
    AuditConfig config4 = AuditConfigManager.buildFromClusterConfig(properties, "pinot.audit.controller");
    assertThat(config4.getCaptureRequestHeaders()).isEqualTo("Content-Type,X-Request-ID,User-Agent,X-Custom-123");
  }

  @Test
  public void testControllerConfigIgnoresBrokerConfig() {
    // Given - Both controller and broker configs are present
    Map<String, String> properties = new HashMap<>();

    // Controller configs (should be read)
    properties.put("pinot.audit.controller.enabled", "true");
    properties.put("pinot.audit.controller.capture.request.payload.enabled", "true");
    properties.put("pinot.audit.controller.capture.request.headers", "X-Controller-Header");
    properties.put("pinot.audit.controller.request.payload.size.max.bytes", "10000");
    properties.put("pinot.audit.controller.url.filter.exclude.patterns", "/controller/health");

    // Broker configs (should be ignored by controller)
    properties.put("pinot.audit.broker.enabled", "false");
    properties.put("pinot.audit.broker.capture.request.payload.enabled", "false");
    properties.put("pinot.audit.broker.capture.request.headers", "X-Broker-Header");
    properties.put("pinot.audit.broker.request.payload.size.max.bytes", "5000");
    properties.put("pinot.audit.broker.url.filter.exclude.patterns", "/broker/health");

    AuditConfigManager controllerManager = new AuditConfigManager(ServiceRole.CONTROLLER);

    // When
    controllerManager.onChange(properties.keySet(), properties);

    // Then - Verify controller reads only controller configs, ignoring broker configs
    AuditConfig config = controllerManager.getCurrentConfig();
    assertThat(config.isEnabled()).isTrue(); // From controller config, not broker
    assertThat(config.isCaptureRequestPayload()).isTrue(); // From controller config, not broker
    assertThat(config.getCaptureRequestHeaders()).isEqualTo("X-Controller-Header"); // Controller header, not broker
    assertThat(config.getMaxPayloadSize()).isEqualTo(10000); // Controller size, not broker
    assertThat(config.getUrlFilterExcludePatterns()).isEqualTo("/controller/health"); // Controller pattern, not broker
  }
}
