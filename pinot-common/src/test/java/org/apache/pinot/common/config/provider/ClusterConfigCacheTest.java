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
package org.apache.pinot.common.config.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises the shared cluster config cache directly and through its Helix callback adapter.
public class ClusterConfigCacheTest {
  @DataProvider
  public Object[][] backends() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "backends")
  public void testSnapshotsAndChanges(boolean helix) {
    ClusterConfigCache cache = createCache(helix);
    List<Set<String>> changes = new ArrayList<>();
    List<Map<String, String>> snapshots = new ArrayList<>();
    cache.registerClusterConfigChangeListener((changed, snapshot) -> {
      changes.add(changed);
      snapshots.add(snapshot);
    });
    assertEquals(snapshots, List.of(Map.of()));
    assertTrue(changes.get(0).isEmpty());

    Map<String, String> input = new HashMap<>(Map.of("key", "old", "removed", "value"));
    update(cache, input);
    Map<String, String> initialSnapshot = cache.getClusterConfigs();
    assertEquals(changes.get(1), Set.of("key", "removed"));
    assertSame(snapshots.get(1), initialSnapshot);
    input.put("key", "mutated");
    assertEquals(initialSnapshot, Map.of("key", "old", "removed", "value"));
    assertThrows(UnsupportedOperationException.class, () -> initialSnapshot.put("key", "mutated"));

    List<Set<String>> lateChanges = new ArrayList<>();
    cache.registerClusterConfigChangeListener((changed, snapshot) -> {
      lateChanges.add(changed);
      assertSame(snapshot, cache.getClusterConfigs());
    });
    assertEquals(lateChanges, List.of(Set.of("key", "removed")));
    update(cache, Map.of("key", "new", "added", "value"));
    assertEquals(changes.get(2), Set.of("key", "removed", "added"));

    input = new HashMap<>(Map.of("added", "value"));
    input.put("key", null);
    update(cache, input);
    assertEquals(changes.get(3), Set.of("key"));
    assertEquals(cache.getClusterConfigs(), Map.of("added", "value"));

    update(cache, Map.of());
    assertEquals(changes.get(4), Set.of("added"));
    assertTrue(cache.getClusterConfigs().isEmpty());
    assertEquals(initialSnapshot, Map.of("key", "old", "removed", "value"));

    AtomicInteger duplicateCalls = new AtomicInteger();
    PinotClusterConfigChangeListener listener = (changed, snapshot) -> duplicateCalls.incrementAndGet();
    cache.registerClusterConfigChangeListener(listener);
    cache.registerClusterConfigChangeListener(listener);
    assertEquals(duplicateCalls.get(), 2);
    update(cache, Map.of());
    assertEquals(duplicateCalls.get(), 4);
    assertEquals(snapshots.size(), 6);
    assertTrue(changes.get(5).isEmpty());
  }

  @Test(dataProvider = "backends")
  public void testRegistrationPrecedesConcurrentUpdate(boolean helix)
      throws Exception {
    ClusterConfigCache cache = createCache(helix);
    update(cache, Map.of("key", "initial"));
    CountDownLatch initialCallback = new CountDownLatch(1);
    CountDownLatch releaseInitialCallback = new CountDownLatch(1);
    CountDownLatch updateRequested = new CountDownLatch(1);
    List<Map<String, String>> snapshots = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> registration = executor.submit(() -> cache.registerClusterConfigChangeListener((changed, snapshot) -> {
        assertTrue(Thread.holdsLock(cache));
        assertSame(snapshot, cache.getClusterConfigs());
        snapshots.add(snapshot);
        if (snapshots.size() == 1) {
          initialCallback.countDown();
          try {
            assertTrue(releaseInitialCallback.await(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }
      }));
      assertTrue(initialCallback.await(10, TimeUnit.SECONDS));
      Future<?> change = executor.submit(() -> {
        updateRequested.countDown();
        update(cache, Map.of("key", "updated"));
      });
      assertTrue(updateRequested.await(10, TimeUnit.SECONDS));
      releaseInitialCallback.countDown();
      registration.get(10, TimeUnit.SECONDS);
      change.get(10, TimeUnit.SECONDS);
      assertEquals(snapshots, List.of(Map.of("key", "initial"), Map.of("key", "updated")));
    } finally {
      releaseInitialCallback.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ClusterConfigCache createCache(boolean helix) {
    return helix ? new DefaultClusterConfigChangeHandler() : new ClusterConfigCache();
  }

  private static void update(ClusterConfigCache cache, Map<String, String> configs) {
    if (cache instanceof DefaultClusterConfigChangeHandler) {
      ClusterConfig clusterConfig = new ClusterConfig("testCluster");
      clusterConfig.getRecord().setSimpleFields(configs);
      ((DefaultClusterConfigChangeHandler) cache).onClusterConfigChange(clusterConfig, null);
    } else {
      cache.updateClusterConfigs(configs);
    }
  }
}
