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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.response.server.SegmentReloadFailureResponse;
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
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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

    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    ServerReloadJobStatusCacheConfig oldConfig = cache.getCurrentConfig();

    // When - Invalid config should keep old cache
    cache.onChange(properties.keySet(), properties);

    // Then - Should keep old config due to error handling
    ServerReloadJobStatusCacheConfig config = cache.getCurrentConfig();
    assertThat(config).isSameAs(oldConfig);
    assertThat(config.getMaxSize()).isEqualTo(10000);
  }

  @Test
  public void testZookeeperConfigDeletionRevertsToDefaults() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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
  public void testOnChangeSkipsRebuildWhenNoRelevantConfigsChanged() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");

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

  // ========== Tests for recordFailure() and getFailedSegmentDetails() ==========

  @Test
  public void testRecordFailureCreatesJobIfNotExists() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    String jobId = "job-new";
    String segmentName = "segment_123";
    Exception exception = new IOException("Test error");

    // When
    cache.recordFailure(jobId, segmentName, exception);

    // Then
    ReloadJobStatus status = cache.getJobStatus(jobId);
    assertThat(status).isNotNull();
    assertThat(status.getFailureCount()).isEqualTo(1);
    assertThat(status.getFailedSegmentDetails()).hasSize(1);
    assertThat(status.getFailedSegmentDetails().get(0).getSegmentName()).isEqualTo(segmentName);
  }

  @org.testng.annotations.DataProvider(name = "failureLimits")
  public Object[][] failureLimitsProvider() {
    return new Object[][] {
        {5, 10, "default limit (5)"},
        {3, 5, "custom limit (3)"},
        {1, 3, "limit of 1"}
    };
  }

  @Test(dataProvider = "failureLimits")
  public void testRecordFailureRespectsLimit(int limit, int totalFailures, String description) {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    if (limit != 5) {
      Map<String, String> properties = new HashMap<>();
      properties.put("pinot.server.table.reload.status.cache.segment.failure.details.count", String.valueOf(limit));
      cache.onChange(properties.keySet(), properties);
    }

    String jobId = "job-limit-" + limit;

    // When - Record failures
    for (int i = 1; i <= totalFailures; i++) {
      cache.recordFailure(jobId, "segment_" + i, new IOException("Error " + i));
    }

    // Then - Count should equal totalFailures, but only first 'limit' details stored
    ReloadJobStatus status = cache.getJobStatus(jobId);
    assertThat(status.getFailureCount()).isEqualTo(totalFailures);
    assertThat(status.getFailedSegmentDetails()).hasSize(limit);
    assertThat(status.getFailedSegmentDetails().get(0).getSegmentName()).isEqualTo("segment_1");
    assertThat(status.getFailedSegmentDetails().get(limit - 1).getSegmentName()).isEqualTo("segment_" + limit);
  }

  @Test
  public void testRecordFailureConcurrent() throws Exception {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    String jobId = "job-concurrent";
    int threadCount = 10;
    int failuresPerThread = 5;

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // When - Record failures concurrently from multiple threads
    for (int t = 0; t < threadCount; t++) {
      int threadId = t;
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        for (int i = 0; i < failuresPerThread; i++) {
          cache.recordFailure(jobId, "segment_t" + threadId + "_" + i,
              new IOException("Error from thread " + threadId));
        }
      }, executor);
      futures.add(future);
    }

    // Wait for all threads to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Then - All failures should be counted
    ReloadJobStatus status = cache.getJobStatus(jobId);
    assertThat(status.getFailureCount()).isEqualTo(threadCount * failuresPerThread);
    // Only first 5 details stored (default limit)
    assertThat(status.getFailedSegmentDetails()).hasSize(5);
  }

  @Test
  public void testConfigChangeUpdatesMaxFailureDetailsLimit() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    assertThat(cache.getCurrentConfig().getSegmentFailureDetailsCount()).isEqualTo(5);  // Default

    // When - Update limit to 2
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.segment.failure.details.count", "2");
    cache.onChange(properties.keySet(), properties);

    // Then - New config should be applied
    assertThat(cache.getCurrentConfig().getSegmentFailureDetailsCount()).isEqualTo(2);

    // New jobs should use new limit
    String jobId = "job-new-limit";
    for (int i = 1; i <= 5; i++) {
      cache.recordFailure(jobId, "segment_" + i, new IOException("Error " + i));
    }

    ReloadJobStatus newJobStatus = cache.getJobStatus(jobId);
    assertThat(newJobStatus.getFailureCount()).isEqualTo(5);
    assertThat(newJobStatus.getFailedSegmentDetails()).hasSize(2);  // New limit applied
  }

  @Test
  public void testGetOrCreateConcurrent() throws Exception {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    String jobId = "concurrent-job";
    int threadCount = 20;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<CompletableFuture<ReloadJobStatus>> futures = new ArrayList<>();

    // When - Multiple threads try to create the same job
    for (int t = 0; t < threadCount; t++) {
      CompletableFuture<ReloadJobStatus> future = CompletableFuture.supplyAsync(() -> {
        return cache.getOrCreate(jobId);
      }, executor);
      futures.add(future);
    }

    // Wait for all threads to complete
    List<ReloadJobStatus> results = new ArrayList<>();
    for (CompletableFuture<ReloadJobStatus> future : futures) {
      results.add(future.get(10, TimeUnit.SECONDS));
    }
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Then - All threads should get the exact same instance
    ReloadJobStatus firstStatus = results.get(0);
    for (ReloadJobStatus status : results) {
      assertThat(status).isSameAs(firstStatus);
    }
    assertThat(cache.getJobStatus(jobId)).isSameAs(firstStatus);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testGetFailedSegmentDetailsReturnsUnmodifiableList() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    String jobId = "test-job";
    cache.recordFailure(jobId, "segment_1", new IOException("Error"));

    // When - Try to modify the returned list
    ReloadJobStatus status = cache.getJobStatus(jobId);
    List<SegmentReloadFailureResponse> details = status.getFailedSegmentDetails();

    // Then - Should throw UnsupportedOperationException
    details.add(new SegmentReloadFailureResponse());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testRecordFailureWithNullJobId() {
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    cache.recordFailure(null, "segment", new IOException("Error"));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testRecordFailureWithNullSegmentName() {
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    cache.recordFailure("job-1", null, new IOException("Error"));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testRecordFailureWithNullException() {
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    cache.recordFailure("job-1", "segment", null);
  }

  @Test
  public void testEdgeCaseZeroLimit() {
    // Given
    ServerReloadJobStatusCache cache = new ServerReloadJobStatusCache("testServer");
    Map<String, String> properties = new HashMap<>();
    properties.put("pinot.server.table.reload.status.cache.segment.failure.details.count", "0");
    cache.onChange(properties.keySet(), properties);

    String jobId = "job-zero-limit";

    // When - Record failures with zero limit
    cache.recordFailure(jobId, "segment_1", new IOException("Error"));
    cache.recordFailure(jobId, "segment_2", new IOException("Error"));

    // Then - Count should be 2, but no details stored
    ReloadJobStatus zeroLimitStatus = cache.getJobStatus(jobId);
    assertThat(zeroLimitStatus.getFailureCount()).isEqualTo(2);
    assertThat(zeroLimitStatus.getFailedSegmentDetails()).isEmpty();
  }
}
