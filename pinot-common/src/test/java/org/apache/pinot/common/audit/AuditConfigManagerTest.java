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
    properties.put("pinot.audit.enabled", "true");
    properties.put("pinot.audit.capture.request.payload.enabled", "true");
    properties.put("pinot.audit.capture.request.headers", "true");
    properties.put("pinot.audit.payload.size.max.bytes", "20480");
    properties.put("pinot.audit.excluded.endpoints", "/health,/metrics");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onChange(properties.keySet(), properties);

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
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.audit.enabled", "true");
    properties.put("pinot.audit.payload.size.max.bytes", "5000");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onChange(properties.keySet(), properties);

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
    Map<String, String> properties = new HashMap<>();
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");
    AuditConfigManager manager = new AuditConfigManager();

    // When
    manager.onChange(properties.keySet(), properties);

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
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.audit.enabled", "true");
    initialProperties.put("pinot.audit.payload.size.max.bytes", "15000");
    manager.onChange(initialProperties.keySet(), initialProperties);
    assertThat(manager.getCurrentConfig().isEnabled()).isTrue();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(15000);

    // When - Update with new config
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put("pinot.audit.enabled", "false");
    updatedProperties.put("pinot.audit.payload.size.max.bytes", "25000");
    manager.onChange(updatedProperties.keySet(), updatedProperties);

    // Then
    assertThat(manager.getCurrentConfig().isEnabled()).isFalse();
    assertThat(manager.getCurrentConfig().getMaxPayloadSize()).isEqualTo(25000);
  }

  @Test
  public void testBuildFromClusterConfigDirectly() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.audit.enabled", "true");
    properties.put("pinot.audit.capture.request.payload.enabled", "false");
    properties.put("pinot.audit.capture.request.headers", "true");
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    // When
    AuditConfig config = AuditConfigManager.buildFromClusterConfig(properties);

    // Then
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.isCaptureRequestPayload()).isFalse();
    assertThat(config.isCaptureRequestHeaders()).isTrue();
    // Verify defaults for unspecified fields
    assertThat(config.getMaxPayloadSize()).isEqualTo(10240);
    assertThat(config.getExcludedEndpoints()).isEmpty();
  }
}
