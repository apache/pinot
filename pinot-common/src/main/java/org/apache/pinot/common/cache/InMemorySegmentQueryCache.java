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
package org.apache.pinot.common.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * InMemorySegmentQueryCache is an implementation of the SegmentQueryCache interface that uses
 * Guava Cache for storing key-value pairs in memory.
 * The implementation is a LRU based cache with TTL expiration, which evicts the least recently used
 * entries when the cache reaches its maximum size.
 *
 * <p>This class is thread-safe. The underlying Guava Cache handles concurrency for cache operations,
 * and the segment index uses ConcurrentHashMap for thread-safe tracking of keys by segment.</p>
 */
public class InMemorySegmentQueryCache implements SegmentQueryCache {
  private static final int DEFAULT_CACHE_SIZE = 1000;
  private static final int DEFAULT_CACHE_EXPIRATION_TIME_IN_SECONDS = 300; // 5 minutes
  private final Cache<String, Object> _cache;
  // Thread-safe index to track keys by segment name
  private final ConcurrentHashMap<String, Set<String>> _segmentIndex = new ConcurrentHashMap<>();

  public InMemorySegmentQueryCache() {
    this(DEFAULT_CACHE_SIZE, DEFAULT_CACHE_EXPIRATION_TIME_IN_SECONDS);
  }

  public InMemorySegmentQueryCache(int size, long ttl) {
    RemovalListener<String, Object> listener = notification -> {
      String key = notification.getKey();
      if (key == null || !key.contains(SegmentKey.DELIMITER)) {
        return;
      }
      SegmentKey segmentKey = SegmentKey.fromCacheKey(key);
      Set<String> keys = _segmentIndex.get(segmentKey.getSegmentName());
      if (keys != null) {
        keys.remove(segmentKey.getKey());
      }
    };

    _cache = CacheBuilder.newBuilder()
        .expireAfterWrite(ttl, TimeUnit.SECONDS)
        .removalListener(listener)
        .maximumSize(size)
        .build();
  }

  @Override
  public void put(String cacheKey, Object value) {
    _cache.put(cacheKey, value);
  }

  @Override
  public void put(SegmentKey segmentKey, Object value) {
    _segmentIndex.computeIfAbsent(segmentKey.getSegmentName(), k -> ConcurrentHashMap.newKeySet())
        .add(segmentKey.getKey());
    _cache.put(segmentKey.getCompositeKey(), value);
  }

  @Override
  public Object get(String cacheKey) {
    return _cache.getIfPresent(cacheKey);
  }

  @Override
  public Object get(SegmentKey segmentKey) {
    return _cache.getIfPresent(segmentKey.getCompositeKey());
  }

  @Override
  public void invalidateCacheForKey(String cacheKey) {
    _cache.invalidate(cacheKey);
  }

  @Override
  public void invalidateCacheForSegment(String segment) {
    Set<String> keys = _segmentIndex.remove(segment);
    if (keys != null) {
      for (String key : keys) {
        String compositeKey = SegmentQueryCache.getCacheKey(segment, key);
        _cache.invalidate(compositeKey);
      }
    }
  }

  @Override
  public void invalidateCacheForSegmentKey(SegmentKey segmentKey) {
    _cache.invalidate(segmentKey.getCompositeKey());
  }
}
