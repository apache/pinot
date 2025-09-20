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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.config.table.CacheConfig;


public class SegmentQueryCacheFactory {
  private static final Map<String, SegmentQueryCache> CACHE_MAP = new ConcurrentHashMap<>();

  private SegmentQueryCacheFactory() {
  }

  /**
   * Creates a new instance of the QueryCache.
   *
   * @return A new instance of the QueryCache
   */
  public static SegmentQueryCache createQueryCache(String table, CacheConfig cacheConfig) {
    String cacheType = cacheConfig.getType();
    return CACHE_MAP.computeIfAbsent(table, key -> createCache(cacheType, cacheConfig));
  }

  private static SegmentQueryCache createCache(String cacheType, CacheConfig cacheConfig) {
    switch (cacheType) {
      case "InMemory":
        return new InMemorySegmentQueryCache(cacheConfig.getMaxSize(), cacheConfig.getTtl());
      // Add more cache types here as needed
      default:
        throw new IllegalArgumentException("Unknown cache type: " + cacheType);
    }
  }

  public static SegmentQueryCache get(String tableName) {
    return CACHE_MAP.get(tableName);
  }
}
