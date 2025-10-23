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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


/**
 * In-memory cache for tracking reload job status on server side.
 * Phase 1: Only tracks failure count per job.
 *
 * <p>Thread-safe for concurrent access. Uses Guava Cache with LRU eviction
 * and time-based expiration.
 */
@ThreadSafe
public class ServerReloadJobStatusCache {
  private static final Logger LOG = LoggerFactory.getLogger(ServerReloadJobStatusCache.class);

  private final Cache<String, ReloadJobStatus> _cache;

  /**
   * Creates a cache with the given configuration.
   *
   */
  public ServerReloadJobStatusCache() {
    final ServerReloadJobStatusCacheConfig config = new ServerReloadJobStatusCacheConfig();
    _cache = CacheBuilder.newBuilder()
        .maximumSize(config.getMaxSize())
        .expireAfterWrite(config.getTtlDays(), TimeUnit.DAYS)
        .recordStats()
        .build();

    LOG.info("Initialized ReloadJobStatusCache with {}", config);
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

  /**
   * Clears all entries from the cache.
   * Useful for testing.
   */
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
}
