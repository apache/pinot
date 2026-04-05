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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemorySegmentQueryCacheTest {

  @Test
  public void testBasicOperations() {
    // Create cache with default size
    InMemorySegmentQueryCache cache = new InMemorySegmentQueryCache();

    // Test put and get with SegmentKey
    SegmentQueryCache.SegmentKey segKey = new SegmentQueryCache.SegmentKey("seg1", "key1");
    cache.put(segKey, "value1");
    Assert.assertEquals(cache.get(segKey), "value1");

    // Test overwrite
    cache.put(segKey, "value2");
    Assert.assertEquals(cache.get(segKey), "value2");

    // Test null value, expecting a NPE
    try {
      cache.put(new SegmentQueryCache.SegmentKey("seg1", "key2"), null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // Expected
    }

    // Test get non-existent key
    Assert.assertNull(cache.get(new SegmentQueryCache.SegmentKey("seg1", "non-existent-key")));

    // Test invalidate by segment key
    cache.invalidateCacheForSegmentKey(segKey);
    Assert.assertNull(cache.get(segKey));
  }

  @Test
  public void testSegmentInvalidation() {
    InMemorySegmentQueryCache cache = new InMemorySegmentQueryCache();

    // Add multiple keys for the same segment
    SegmentQueryCache.SegmentKey key1 = new SegmentQueryCache.SegmentKey("seg1", "filterA");
    SegmentQueryCache.SegmentKey key2 = new SegmentQueryCache.SegmentKey("seg1", "filterB");
    SegmentQueryCache.SegmentKey key3 = new SegmentQueryCache.SegmentKey("seg2", "filterA");

    cache.put(key1, "val1");
    cache.put(key2, "val2");
    cache.put(key3, "val3");

    // Invalidate all entries for seg1
    cache.invalidateCacheForSegment("seg1");

    // seg1 entries should be gone
    Assert.assertNull(cache.get(key1));
    Assert.assertNull(cache.get(key2));
    // seg2 entry should remain
    Assert.assertEquals(cache.get(key3), "val3");
  }

  @Test
  public void testCacheSizeLimit()
      throws Exception {
    // Create cache with small size
    int cacheSize = 5;
    InMemorySegmentQueryCache cache = new InMemorySegmentQueryCache(cacheSize, 300);

    // Fill cache to its limit
    for (int i = 0; i < cacheSize; i++) {
      cache.put("key" + i, "value" + i);
    }

    // Verify all entries are present
    for (int i = 0; i < cacheSize; i++) {
      Assert.assertEquals(cache.get("key" + i), "value" + i);
    }

    // Add more entries to force eviction
    for (int i = cacheSize; i < cacheSize * 2; i++) {
      cache.put("key" + i, "value" + i);
      // Access some of the earlier entries to keep them in cache
      if (i % 2 == 0 && i >= cacheSize + 2) {
        cache.get("key" + (i - 2));
      }
    }

    // Allow time for async eviction if necessary
    Thread.sleep(100);

    // Verify some of the older, unused entries have been evicted
    int evictedCount = 0;
    for (int i = 0; i < cacheSize; i++) {
      if (cache.get("key" + i) == null) {
        evictedCount++;
      }
    }

    // Some eviction should have occurred
    Assert.assertTrue(evictedCount > 0);
  }

  @Test
  public void testConcurrentAccess()
      throws Exception {
    // Create cache with default size
    final InMemorySegmentQueryCache cache = new InMemorySegmentQueryCache();
    int numThreads = 10;
    int operationsPerThread = 1000;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

    // Create concurrent threads that perform operations
    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          // Each thread works with its own set of keys to avoid test flakiness
          for (int i = 0; i < operationsPerThread; i++) {
            String key = "key-" + threadId + "-" + i;
            String value = "value-" + threadId + "-" + i;

            // Perform put, then get, then remove in sequence for every 3 operations
            int operation = i % 3;
            switch (operation) {
              case 0: // put
                cache.put(key, value);
                break;
              case 1: // get - the previous key (i-1) was a put
                String prevKey = "key-" + threadId + "-" + (i - 1);
                String prevValue = "value-" + threadId + "-" + (i - 1);
                Object retrieved = cache.get(prevKey);
                // The previous put might have been evicted, but if present should match
                if (retrieved != null) {
                  Assert.assertEquals(retrieved, prevValue);
                }
                break;
              case 2: // remove
                String removeKey = "key-" + threadId + "-" + (i - 2);
                cache.invalidateCacheForKey(removeKey);
                // Verify it was removed
                Assert.assertNull(cache.get(removeKey));
                break;
              default:
                Assert.fail("Unexpected operation: " + operation);
                break;
            }
          }
        } catch (Exception e) {
          exceptions.add(e);
        } finally {
          latch.countDown();
        }
      });
    }

    // Wait for all threads to complete
    boolean completed = latch.await(30, TimeUnit.SECONDS);
    executor.shutdown();

    // Verify no exceptions and all threads completed
    Assert.assertTrue(completed, "Not all threads completed in time");
    Assert.assertTrue(exceptions.isEmpty(), "Exceptions occurred during concurrent operations: " + exceptions);
  }

  @Test
  public void testQueryCacheUpdater() {
    InMemorySegmentQueryCache cache = new InMemorySegmentQueryCache();
    String segmentName = "testSegment";
    SegmentQueryCache.SegmentKey segmentKey = new SegmentQueryCache.SegmentKey(segmentName, "key");

    // Create cache updater
    SegmentQueryCache.QueryCacheUpdater updater = new SegmentQueryCache.QueryCacheUpdater(cache, segmentKey);

    // Update cache via updater
    String value = "test-value";
    updater.update(value);

    // Verify cache was updated
    Assert.assertEquals(cache.get(segmentKey), value);

    // Update again
    String newValue = "new-test-value";
    updater.update(newValue);

    // Verify cache was updated with new value
    Assert.assertEquals(cache.get(segmentKey), newValue);
  }

  @Test
  public void testCacheKeyGeneration() {
    FilterContext filterContext =
        FilterContext.forPredicate(new EqPredicate(ExpressionContext.forIdentifier("columnA"), "aaa"));
    String segmentName = "testSegment";
    String cacheKey = SegmentQueryCache.getCacheKey(segmentName, filterContext);

    // Verify cache key format: segmentName + delimiter + hashCode
    Assert.assertTrue(cacheKey.startsWith(segmentName + SegmentQueryCache.SegmentKey.DELIMITER));

    // Verify that equal filters produce equal cache keys
    FilterContext filterContext2 =
        FilterContext.forPredicate(new EqPredicate(ExpressionContext.forIdentifier("columnA"), "aaa"));
    String cacheKey2 = SegmentQueryCache.getCacheKey(segmentName, filterContext2);
    Assert.assertEquals(cacheKey, cacheKey2);

    // Verify that different filters produce different cache keys
    FilterContext filterContext3 =
        FilterContext.forPredicate(new EqPredicate(ExpressionContext.forIdentifier("columnB"), "bbb"));
    String cacheKey3 = SegmentQueryCache.getCacheKey(segmentName, filterContext3);
    Assert.assertNotEquals(cacheKey, cacheKey3);
  }
}
