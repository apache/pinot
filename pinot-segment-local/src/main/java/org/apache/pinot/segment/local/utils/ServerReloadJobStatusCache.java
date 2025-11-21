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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.response.server.ApiErrorResponse;
import org.apache.pinot.common.response.server.SegmentReloadFailureResponse;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


/**
 * In-memory cache for tracking reload job status on server side.
 *
 * <p>Thread-safe for concurrent access. Uses Guava Cache with LRU eviction
 * and time-based expiration.
 *
 * <p>Implements PinotClusterConfigChangeListener to support dynamic configuration
 * updates from ZooKeeper cluster config. When config changes, cache is rebuilt
 * with new settings and existing entries are migrated.
 */
@ThreadSafe
public class ServerReloadJobStatusCache implements PinotClusterConfigChangeListener {
  private static final Logger LOG = LoggerFactory.getLogger(ServerReloadJobStatusCache.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static final String CONFIG_PREFIX = "pinot.server.table.reload.status.cache";

  private final String _instanceId;
  private volatile Cache<String, ReloadJobStatus> _cache;
  private volatile ServerReloadJobStatusCacheConfig _currentConfig;

  public ServerReloadJobStatusCache(String instanceId) {
    _instanceId = requireNonNull(instanceId, "instanceId cannot be null");
    _currentConfig = new ServerReloadJobStatusCacheConfig();
    _cache = CacheBuilder.newBuilder()
        .maximumSize(_currentConfig.getMaxSize())
        .expireAfterWrite(_currentConfig.getTtlDays(), TimeUnit.DAYS)
        .recordStats()
        .build();

    LOG.info("Initialized ReloadJobStatusCache for instance {} with {}", _instanceId, _currentConfig);
  }

  /**
   * Gets the complete job status for the given job.
   *
   * @param jobId Reload job ID (UUID)
   * @return Job status, or null if not found in cache
   */
  @Nullable
  public ReloadJobStatus getJobStatus(String jobId) {
    requireNonNull(jobId, "jobId cannot be null");

    return _cache.getIfPresent(jobId);
  }

  public void clear() {
    _cache.invalidateAll();
    LOG.info("Cleared all entries from reload job status cache");
  }

  /**
   * Gets or creates a job status entry atomically.
   */
  public ReloadJobStatus getOrCreate(String jobId) {
    ReloadJobStatus status = _cache.getIfPresent(jobId);
    if (status == null) {
      synchronized (this) {
        status = _cache.getIfPresent(jobId);
        if (status == null) {
          status = new ReloadJobStatus(jobId);
          _cache.put(jobId, status);
          LOG.debug("Created new job status entry for job: {}", jobId);
        }
      }
    }
    return status;
  }

  /**
   * Records a segment reload failure in the cache.
   * Handles all business logic: counting, limit enforcement, thread safety.
   *
   * <p>This method ALWAYS increments the failure count, but only stores detailed
   * failure information (segment name, exception, stack trace) for the first N failures
   * where N is configured by maxFailureDetailsToCapture.
   *
   * @param jobId reload job ID (UUID)
   * @param segmentName name of failed segment
   * @param exception the exception that caused the failure
   */
  public void recordFailure(String jobId, String segmentName, Throwable exception) {
    requireNonNull(jobId, "jobId cannot be null");
    requireNonNull(segmentName, "segmentName cannot be null");
    requireNonNull(exception, "exception cannot be null");

    ReloadJobStatus status = getOrCreate(jobId);
    status.incrementAndGetFailureCount();

    synchronized (status) {
      int maxLimit = _currentConfig.getSegmentFailureDetailsCount();
      if (status.getFailedSegmentDetails().size() < maxLimit) {
        status.addFailureDetail(new SegmentReloadFailureResponse()
            .setSegmentName(segmentName)
            .setServerName(_instanceId)
            .setError(new ApiErrorResponse()
                .setErrorMsg(exception.getMessage())
                .setStacktrace(ExceptionUtils.getStackTrace(exception)))
            .setFailedAtMs(System.currentTimeMillis()));
      }
    }
  }

  /**
   * Rebuilds the cache with new configuration and migrates existing entries.
   * This method is synchronized to prevent concurrent rebuilds.
   *
   * @param newConfig new cache configuration to apply
   */
  private synchronized void rebuildCache(ServerReloadJobStatusCacheConfig newConfig) {
    LOG.info("Rebuilding reload status cache with new config: {}", newConfig);

    // Create new cache with new configuration
    Cache<String, ReloadJobStatus> newCache = CacheBuilder.newBuilder()
        .maximumSize(newConfig.getMaxSize())
        .expireAfterWrite(newConfig.getTtlDays(), TimeUnit.DAYS)
        .recordStats()
        .build();

    // Migrate existing entries from old cache to new cache
    Cache<String, ReloadJobStatus> oldCache = _cache;
    if (oldCache != null) {
      newCache.putAll(oldCache.asMap());
    }

    _cache = newCache;
    _currentConfig = newConfig;

    LOG.info("Successfully rebuilt reload status cache (size: {})", newCache.size());
  }

  /**
   * Maps cluster configuration properties with a common prefix to a config POJO using Jackson.
   * Uses PinotConfiguration.subset() to extract properties with the given prefix and
   * Jackson's convertValue() for automatic object mapping.
   *
   * @param clusterConfigs map of all cluster configs from ZooKeeper
   * @param configPrefix prefix to filter configs (e.g., "pinot.server.table.reload.status.cache")
   * @return ServerReloadJobStatusCacheConfig with values from cluster config, defaults for missing values
   */
  @VisibleForTesting
  static ServerReloadJobStatusCacheConfig buildFromClusterConfig(Map<String, String> clusterConfigs,
      String configPrefix) {
    final MapConfiguration mapConfig = new MapConfiguration(clusterConfigs);
    final PinotConfiguration subsetConfig = new PinotConfiguration(mapConfig).subset(configPrefix);
    return OBJECT_MAPPER.convertValue(subsetConfig.toMap(), ServerReloadJobStatusCacheConfig.class);
  }

  @VisibleForTesting
  public ServerReloadJobStatusCacheConfig getCurrentConfig() {
    return _currentConfig;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    boolean hasRelevantChanges = changedConfigs.stream()
        .anyMatch(key -> key.startsWith(CONFIG_PREFIX));

    if (!hasRelevantChanges) {
      LOG.info("No reload cache config changes detected, skipping rebuild");
      return;
    }

    try {
      ServerReloadJobStatusCacheConfig newConfig = buildFromClusterConfig(clusterConfigs, CONFIG_PREFIX);
      rebuildCache(newConfig);
      LOG.info("Successfully rebuilt cache with updated configuration");
    } catch (Exception e) {
      LOG.error("Failed to rebuild cache from cluster config, keeping existing cache", e);
    }
  }
}
