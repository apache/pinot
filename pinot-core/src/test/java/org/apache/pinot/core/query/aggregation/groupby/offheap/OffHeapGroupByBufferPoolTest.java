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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class OffHeapGroupByBufferPoolTest {

  @AfterMethod
  public void resetPool() {
    OffHeapGroupByBufferPool.clearCurrentThread();
    OffHeapGroupByBufferPool.setMaxBytesPerThread(0);
  }

  @Test
  public void testDisabledPoolIsPassThrough() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    PinotDataBuffer buffer = OffHeapGroupByBufferPool.acquire(4096, "test");
    assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline);
    OffHeapGroupByBufferPool.release(buffer);
    // Disabled pool closes on release: usage returns to baseline and nothing is retained
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 0);
  }

  @Test
  public void testEnabledPoolReusesExactSize() {
    OffHeapGroupByBufferPool.setMaxBytesPerThread(1 << 20);
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    PinotDataBuffer first = OffHeapGroupByBufferPool.acquire(8192, "test");
    OffHeapGroupByBufferPool.release(first);
    // Pooled buffer stays open and accounted
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline + 8192);
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 8192);
    // Exact-size acquire reuses the same buffer instance; a different size allocates fresh
    PinotDataBuffer reused = OffHeapGroupByBufferPool.acquire(8192, "test");
    assertSame(reused, first);
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 0);
    PinotDataBuffer other = OffHeapGroupByBufferPool.acquire(4096, "test");
    assertNotSame(other, first);
    OffHeapGroupByBufferPool.release(reused);
    OffHeapGroupByBufferPool.release(other);
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 8192 + 4096);
    // clearCurrentThread closes everything
    OffHeapGroupByBufferPool.clearCurrentThread();
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 0);
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }

  @Test
  public void testPerThreadCapEvicts() {
    OffHeapGroupByBufferPool.setMaxBytesPerThread(10_000);
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    PinotDataBuffer first = OffHeapGroupByBufferPool.acquire(8192, "test");
    PinotDataBuffer second = OffHeapGroupByBufferPool.acquire(8192, "test");
    OffHeapGroupByBufferPool.release(first);
    // Second release would exceed the 10_000-byte cap: the buffer is closed instead of pooled
    OffHeapGroupByBufferPool.release(second);
    assertEquals(OffHeapGroupByBufferPool.getPooledBytes(), 8192);
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline + 8192);
    OffHeapGroupByBufferPool.clearCurrentThread();
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }

  @Test
  public void testStructuresRunCorrectlyWithPoolEnabled() {
    OffHeapGroupByBufferPool.setMaxBytesPerThread(16 << 20);
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    // Run a map through two full lifecycles: the second run reuses dirty pooled buffers, so any missing
    // re-initialization (zero-fill) would corrupt its results
    for (int run = 0; run < 2; run++) {
      try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
        for (int key = 0; key < 50_000; key++) {
          assertEquals(map.getGroupId(key, Integer.MAX_VALUE), key, "run " + run + " key " + key);
        }
        for (int key = 0; key < 50_000; key++) {
          assertEquals(map.getGroupId(key, Integer.MAX_VALUE), key, "run " + run + " lookup " + key);
        }
      }
    }
    assertTrue(OffHeapGroupByBufferPool.getPooledBytes() > 0);
    OffHeapGroupByBufferPool.clearCurrentThread();
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }
}
