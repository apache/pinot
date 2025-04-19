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

import java.util.Objects;
import org.apache.pinot.common.request.context.FilterContext;


public interface SegmentQueryCache {

  /**
   * Put a key-value pair into the cache
   *
   * @param cacheKey   The key to be put into the cache
   * @param value The value to be put into the cache
   */
  void put(String cacheKey, Object value);

  /**
   * Put a key-value pair into the cache for the specified segment.
   *
   * @param segmentKey The segment key to be put into the cache
   * @param value The value to be put into the cache
   */
  void put(SegmentKey segmentKey, Object value);

  /**
   * Get the value for the given key from the cache.
   *
   * @param cacheKey The key to get from the cache
   * @return The value for the given key, or null if not found
   */
  Object get(String cacheKey);

  /**
   * Get the value for the given key from the cache for the specified segment.
   *
   * @param segmentKey The segment key to get from the cache
   * @return The value for the given key, or null if not found
   */
  Object get(SegmentKey segmentKey);

  /**
   * Clear cache for a given segment.
   *
   * @param cacheKey The cache key to remove from the cache
   */
  void invalidateCacheForKey(String cacheKey);

  /**
   * Invalidate cache for the specified segment.
   *
   * @param segment The segment name from which to remove the key
   */
  void invalidateCacheForSegment(String segment);

  /**
   * Invalidate cache for the specified segment key.
   *
   * @param segmentKey The segment key for which to remove the cache entry
   */
  void invalidateCacheForSegmentKey(SegmentKey segmentKey);

  /**
   * Get the cache key for a filter context.
   *
   * @param filterContext The filter context
   * @return The cache key
   */
  static String getCacheKey(FilterContext filterContext) {
    return String.valueOf(filterContext.hashCode());
  }

  /**
   * Get the cache key for the given table, segment name, and filter context.
   *
   * @param segmentName The name of the segment
   * @param filterContext The filter context
   * @return The cache key
   */
  static String getCacheKey(String segmentName, FilterContext filterContext) {
    return String.format("%s%s%d", segmentName, SegmentKey.DELIMITER, filterContext.hashCode());
  }

  /**
   * Get the cache key for the given table, segment name, and key.
   *
   * @param segmentName The name of the segment
   * @param key a key to the segment
   * @return The cache key
   */
  static String getCacheKey(String segmentName, String key) {
    return String.format("%s%s%s", segmentName, SegmentKey.DELIMITER, key);
  }

  class QueryCacheUpdater {
    private final SegmentQueryCache _queryCache;
    private final SegmentKey _segmentKey;

    public QueryCacheUpdater(SegmentQueryCache queryCache, SegmentKey segmentKey) {
      _queryCache = queryCache;
      _segmentKey = segmentKey;
    }

    public void update(Object value) {
      _queryCache.put(_segmentKey, value);
    }
  }

  // Composite key
  class SegmentKey {
    public static final String DELIMITER = "\u001F"; // Unit separator
    final String _segmentName;
    final String _key; // Or any filter object

    public SegmentKey(String segmentName, String key) {
      _segmentName = segmentName;
      _key = key;
    }

    public SegmentKey(String segmentName, FilterContext filterContext) {
      this(segmentName, getCacheKey(filterContext));
    }

    public static SegmentKey fromCacheKey(String compositeKey) {
      String[] parts = compositeKey.split(DELIMITER, 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid cache key format: " + compositeKey);
      }
      return new SegmentKey(parts[0], parts[1]);
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public String getKey() {
      return _key;
    }

    public String getCompositeKey() {
      if (_segmentName == null || _key == null) {
        throw new IllegalArgumentException("Segment name and key cannot be null");
      }
      return _segmentName + DELIMITER + _key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SegmentKey)) {
        return false;
      }
      SegmentKey that = (SegmentKey) o;
      return Objects.equals(_segmentName, that._segmentName)
          && Objects.equals(_key, that._key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_segmentName, _key);
    }
  }
}
