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

import org.apache.pinot.common.request.context.FilterContext;


public interface QueryCache {
  /**
   * Put the given key-value pair into the cache.
   *
   * @param key   The key to be put into the cache
   * @param value The value to be put into the cache
   */
  void put(String key, Object value);

  /**
   * Get the value for the given key from the cache.
   *
   * @param key The key to get from the cache
   * @return The value for the given key, or null if not found
   */
  Object get(String key);

  /**
   * Remove the given key from the cache.
   *
   * @param key The key to remove from the cache
   */
  void remove(String key);

  /**
   * Get the cache key for the given table, segment name, and filter context.
   * @param segmentName The segment name
   * @param filterContext The filter context
   * @return The cache key
   */
  static String getCacheKey(String segmentName, FilterContext filterContext) {
    return segmentName + "_" + filterContext.hashCode();
  }

  class QueryCacheUpdater {
    private final QueryCache _queryCache;
    private final String _key;

    public QueryCacheUpdater(QueryCache queryCache, String cacheKey) {
      _queryCache = queryCache;
      _key = cacheKey;
    }

    public void update(Object value) {
      _queryCache.put(_key, value);
    }
  }
}
