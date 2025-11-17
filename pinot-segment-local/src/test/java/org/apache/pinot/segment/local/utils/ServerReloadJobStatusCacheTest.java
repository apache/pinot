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
package org.apache.pinot.segment.local.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for ServerReloadJobStatusCache to verify correct config injection
 * when onChange is called, cache rebuild logic, and entry migration.
 */
public class ServerReloadJobStatusCacheTest {

  @Test
  public void testDefaultConfigInitialization() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // Then
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config).isNotNull();
    assertThat(config.getMaxSize()).isEqualTo(10000);
    assertThat(config.getTtlDays()).isEqualTo(30);
  }

  @Test
  public void testOnChangeWithFullConfig() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "5000");
    properties.put("pinot.server.table.reload.status.cache.ttl.days", "15");
    properties.put("some.other.config", "value");

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // When
    cache.onChange(properties.keySet(), properties);

    // Then
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config.getMaxSize()).isEqualTo(5000);
    assertThat(config.getTtlDays()).isEqualTo(15);
  }

  @Test
  public void testOnChangeWithPartialConfig() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "7500");
    properties.put("some.other.config", "value");

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // When
    cache.onChange(properties.keySet(), properties);

    // Then
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config.getMaxSize()).isEqualTo(7500);
    // Verify default for unspecified config
    assertThat(config.getTtlDays()).isEqualTo(30);
  }

  @Test
  public void testOnChangeWithNoRelevantConfigs() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("some.other.config", "value");
    properties.put("another.config", "123");

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // When
    cache.onChange(properties.keySet(), properties);

    // Then - Should keep defaults
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config.getMaxSize()).isEqualTo(10000);
    assertThat(config.getTtlDays()).isEqualTo(30);
  }

  @Test
  public void testOnChangeWithInvalidValues() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "invalid");

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();
    ServerReloadJobStatusCacheConfig oldConfig = cache.getCurrentConfig();

    // When - Invalid config should keep old cache
    cache.onChange(properties.keySet(), properties);

    // Then - Should keep old config due to error handling
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config).isSameAs(oldConfig);
    assertThat(config.getMaxSize()).isEqualTo(10000);
  }

  @Test
  public void testConfigUpdateOverwritesPrevious() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // Set initial config
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.server.table.reload.status.cache.size.max", "8000");
    initialProperties.put("pinot.server.table.reload.status.cache.ttl.days", "20");
    cache.onChange(initialProperties.keySet(), initialProperties);
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(8000);
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(20);

    // When - Update with new config
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put("pinot.server.table.reload.status.cache.size.max", "12000");
    updatedProperties.put("pinot.server.table.reload.status.cache.ttl.days", "45");
    cache.onChange(updatedProperties.keySet(), updatedProperties);

    // Then
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(12000);
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(45);
  }

  @Test
  public void testZookeeperConfigDeletionRevertsToDefaults() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // Set initial custom configs
    Map<String, String> customProperties = new HashMap<>();
    customProperties.put("pinot.server.table.reload.status.cache.size.max", "15000");
    customProperties.put("pinot.server.table.reload.status.cache.ttl.days", "60");
    cache.onChange(customProperties.keySet(), customProperties);

    // Verify custom configs are applied
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(15000);
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(60);

    // When - Simulate ZooKeeper config deletion with empty map
    Map<String, String> emptyProperties = new HashMap<>();
    cache.onChange(customProperties.keySet(), emptyProperties);

    // Then - Verify all configs revert to defaults
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(10000);
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(30);
  }

  @Test
  public void testBuildFromClusterConfigDirectly() {
    // Given
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "6000");
    properties.put("pinot.server.table.reload.status.cache.ttl.days", "25");
    properties.put("some.other.config", "value");

    // When
    ServerReloadJobStatusCacheConfig config =
        ServerReloadJobStatusCache.buildFromClusterConfig(properties, ServerReloadJobStatusCache.CONFIG_PREFIX);

    // Then
    assertThat(config.getMaxSize()).isEqualTo(6000);
    assertThat(config.getTtlDays()).isEqualTo(25);
  }

  @Test
  public void testCacheEntryMigrationOnRebuild() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    // Add some entries to cache
    ReloadJobStatus status1 = cache.getOrCreate("job-1");
    status1.incrementAndGetFailureCount();
    ReloadJobStatus status2 = cache.getOrCreate("job-2");
    status2.incrementAndGetFailureCount();
    status2.incrementAndGetFailureCount();

    // Verify initial state
    assertThat(cache.getJobStatus("job-1").getFailureCount()).isEqualTo(1);
    assertThat(cache.getJobStatus("job-2").getFailureCount()).isEqualTo(2);

    // When - Trigger cache rebuild with config change
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "5000");
    properties.put("pinot.server.table.reload.status.cache.ttl.days", "15");
    cache.onChange(properties.keySet(), properties);

    // Then - Entries should be migrated to new cache
    assertThat(cache.getJobStatus("job-1")).isNotNull();
    assertThat(cache.getJobStatus("job-1").getFailureCount()).isEqualTo(1);
    assertThat(cache.getJobStatus("job-2")).isNotNull();
    assertThat(cache.getJobStatus("job-2").getFailureCount()).isEqualTo(2);

    // Verify new config is applied
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(5000);
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(15);
  }

  @Test
  public void testCacheRebuildWithDifferentSize() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(10000);

    // When - Update only max size
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.size.max", "20000");
    cache.onChange(properties.keySet(), properties);

    // Then - Verify new size takes effect
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(20000);
    // TTL should remain default
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(30);
  }

  @Test
  public void testCacheRebuildWithDifferentTTL() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(30);

    // When - Update only TTL
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.ttl.days", "45");
    cache.onChange(properties.keySet(), properties);

    // Then - Verify new TTL takes effect
    assertThat(cache.getCurrentConfig().getTtlDays()).isEqualTo(45);
    // Max size should remain default
    assertThat(cache.getCurrentConfig().getMaxSize()).isEqualTo(10000);
  }

  @Test
  public void testOnChangeSkipsRebuildWhenNoRelevantConfigsChanged() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.server.table.reload.status.cache.size.max", "8000");
    initialProperties.put("pinot.server.table.reload.status.cache.ttl.days", "20");
    cache.onChange(initialProperties.keySet(), initialProperties);

    // Capture initial config instance
    ServerReloadJobStatusCacheConfig configBeforeChange = cache.getCurrentConfig();
    assertThat(configBeforeChange.getMaxSize()).isEqualTo(8000);
    assertThat(configBeforeChange.getTtlDays()).isEqualTo(20);

    // When - Update with only non-reload-cache configs
    Map<String, String> nonRelevantProperties = new HashMap<>();
    nonRelevantProperties.put("some.other.config", "newValue");
    nonRelevantProperties.put("another.config", "456");
    // Include the previous reload cache configs to simulate cluster state
    nonRelevantProperties.putAll(initialProperties);

    cache.onChange(Set.of("some.other.config", "another.config"), nonRelevantProperties);

    // Then - Config instance should be the exact same object (no rebuild occurred)
    ServerReloadJobStatusCacheConfig configAfterChange = cache.getCurrentConfig();
    assertThat(configAfterChange).isSameAs(configBeforeChange);
  }

  @Test
  public void testOnChangeRebuildsWhenRelevantConfigsChanged() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache();

    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("pinot.server.table.reload.status.cache.size.max", "8000");
    initialProperties.put("pinot.server.table.reload.status.cache.ttl.days", "20");
    cache.onChange(initialProperties.keySet(), initialProperties);

    ServerReloadJobStatusCacheConfig configBefore = cache.getCurrentConfig();
    assertThat(configBefore.getMaxSize()).isEqualTo(8000);
    assertThat(configBefore.getTtlDays()).isEqualTo(20);

    // When - Update with reload cache configs changed
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.put("pinot.server.table.reload.status.cache.size.max", "12000");
    updatedProperties.put("pinot.server.table.reload.status.cache.ttl.days", "40");
    updatedProperties.put("some.other.config", "value");

    cache.onChange(
        Set.of("pinot.server.table.reload.status.cache.size.max", "pinot.server.table.reload.status.cache.ttl.days"),
        updatedProperties);

    // Then - Config should be rebuilt with new values
    ServerReloadJobStatusCacheConfig configAfter = cache.getCurrentConfig();
    assertThat(configAfter).isNotSameAs(configBefore);
    assertThat(configAfter.getMaxSize()).isEqualTo(12000);
    assertThat(configAfter.getTtlDays()).isEqualTo(40);
  }
}
