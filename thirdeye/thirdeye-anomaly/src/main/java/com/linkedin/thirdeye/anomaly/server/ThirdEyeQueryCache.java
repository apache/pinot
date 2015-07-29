package com.linkedin.thirdeye.anomaly.server;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU cache
 */
public class ThirdEyeQueryCache<K, V> {

  private final Map<K, V> cache;

  public ThirdEyeQueryCache(final int maxSize) {
    cache = (Map<K, V>) Collections.synchronizedMap(
        new LinkedHashMap<K, V>(maxSize + 1, .75F, true) {
          public boolean removeEldestEntry(Map.Entry eldest) {
            return size() > maxSize;
          }
        });
  }

  public V get(K key) {
    return cache.get(key);
  }

  public void put(K key, V value) {
    cache.put(key, value);
  }

}
