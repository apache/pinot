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
import java.util.concurrent.TimeUnit;


/**
 * InMemoryQueryCache is a simple implementation of the QueryCache interface that uses
 * Guava Cache for storing key-value pairs in memory.
 * The implementation is a LRU based cache, which means that it will evict the least recently used
 * entries when the cache reaches its maximum size.
 */
public class InMemoryQueryCache implements QueryCache {
  private static final int DEFAULT_CACHE_SIZE = 1000;
  private static final int DEFAULT_CACHE_EXPIRATION_TIME_IN_SECONDS = 300; // 5 minutes
  private final Cache<String, Object> _cache;

  public InMemoryQueryCache() {
    this(DEFAULT_CACHE_SIZE, DEFAULT_CACHE_EXPIRATION_TIME_IN_SECONDS);
  }

  public InMemoryQueryCache(int cacheSize) {
    this(cacheSize, DEFAULT_CACHE_EXPIRATION_TIME_IN_SECONDS);
  }

  public InMemoryQueryCache(int size, long ttl) {
    _cache = CacheBuilder.newBuilder()
        .expireAfterAccess(ttl, TimeUnit.SECONDS)
        .maximumSize(size)
        .build();
  }

  @Override
  public void put(String key, Object value) {
    _cache.put(key, value);
  }

  @Override
  public Object get(String key) {
    return _cache.getIfPresent(key);
  }

  @Override
  public void remove(String key) {
    _cache.invalidate(key);
  }
}
