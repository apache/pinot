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
package org.apache.pinot.plugin.stream.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class KafkaAdminClientManagerExactOnceTest {

  @AfterMethod
  public void tearDown() {
    // Best-effort cleanup: clear the manager cache to avoid cross-test interference
    try {
      KafkaAdminClientManager manager = KafkaAdminClientManager.getInstance();
      Field mapField = KafkaAdminClientManager.class.getDeclaredField("_adminClients");
      mapField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, ?> map = (Map<String, ?>) mapField.get(manager);
      map.clear();
    } catch (Exception ignored) {
      // ignore
    }
  }

  @Test
  public void testRemovalHappensExactlyOnceUnderConcurrentCloses()
      throws Exception {
    KafkaAdminClientManager manager = KafkaAdminClientManager.getInstance();

    // Build properties and compute the cache key via private method
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");

    Method createCacheKey = KafkaAdminClientManager.class.getDeclaredMethod("createCacheKey", Properties.class);
    createCacheKey.setAccessible(true);
    String cacheKey = (String) createCacheKey.invoke(manager, props);

    // Pre-insert a wrapper into the manager map with a null AdminClient. This allows us to
    // exercise the reference counting and removal logic without needing a live Kafka cluster
    // or additional mocking libraries.
    Class<?>[] declared = KafkaAdminClientManager.class.getDeclaredClasses();
    Class<?> wrapperClass = null;
    for (Class<?> c : declared) {
      if (c.getSimpleName().equals("AdminClientWrapper")) {
        wrapperClass = c;
        break;
      }
    }
    assertNotNull(wrapperClass, "AdminClientWrapper class should be present");

    Constructor<?> ctor = wrapperClass.getDeclaredConstructor(KafkaAdminClientManager.class,
        org.apache.kafka.clients.admin.AdminClient.class,
        String.class);
    ctor.setAccessible(true);
    Object wrapper = ctor.newInstance(manager, null, cacheKey);

    Field mapField = KafkaAdminClientManager.class.getDeclaredField("_adminClients");
    mapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) mapField.get(manager);
    map.put(cacheKey, wrapper);

    // Acquire multiple references for the same key
    int numRefs = 16;
    List<KafkaAdminClientManager.AdminClientReference> refs = new ArrayList<>(numRefs);
    for (int i = 0; i < numRefs; i++) {
      refs.add(manager.getOrCreateAdminClient(props));
    }

    // Close all references concurrently
    ExecutorService pool = Executors.newFixedThreadPool(8);
    CountDownLatch startLatch = new CountDownLatch(1);
    for (KafkaAdminClientManager.AdminClientReference ref : refs) {
      pool.submit(() -> {
        try {
          startLatch.await();
          ref.close();
        } catch (InterruptedException ignored) {
        }
      });
    }
    startLatch.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "Close tasks did not finish in time");

    // Verify the cache entry is removed after all references are closed
    assertFalse(map.containsKey(cacheKey), "Cache entry should be removed after last close");

    // Extra safety: closing already-closed references should be no-ops
    for (KafkaAdminClientManager.AdminClientReference ref : refs) {
      ref.close();
    }

    // Cache must remain empty for this key
    assertFalse(map.containsKey(cacheKey), "Cache entry should remain removed");
  }
}
