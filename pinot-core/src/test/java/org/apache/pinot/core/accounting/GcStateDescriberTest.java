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
package org.apache.pinot.core.accounting;

import com.sun.management.GcInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Feeds [GcStateDescriber] with fake MXBeans. The collector and pool names are the ones each JDK collector reports.
public class GcStateDescriberTest {
  private static final long UPTIME_MS = 1_000_000;

  @Test
  public void testDescribeG1() {
    List<MemoryPoolMXBean> pools = List.of(pool("G1 Eden Space", MemoryType.HEAP, 0L),
        pool("G1 Survivor Space", MemoryType.HEAP, 10L), pool("G1 Old Gen", MemoryType.HEAP, 400L),
        pool("Metaspace", MemoryType.NON_HEAP, null));
    List<GarbageCollectorMXBean> collectors = List.of(collector("G1 Young Generation", 10, UPTIME_MS - 120),
        collector("G1 Concurrent GC", 2, UPTIME_MS - 1_300), collector("G1 Old Generation", 0, null));
    GcStateDescriber describer = new GcStateDescriber(collectors, pools, () -> UPTIME_MS);

    assertEquals(describer.describe(), "oldGenUsedAfterLastGcBytes: 400, collectors: ["
        + "G1 Young Generation (count: 10, last ended 120 ms ago), "
        + "G1 Concurrent GC (count: 2, last ended 1300 ms ago), "
        + "G1 Old Generation (count: 0)]");
  }

  @Test
  public void testDescribeWithoutGcDetails() {
    // A collector bean outside com.sun.management exposes no last GC info, and a pool that was never collected
    // reports no collection usage
    GarbageCollectorMXBean collector = mock(GarbageCollectorMXBean.class);
    when(collector.getName()).thenReturn("Copy");
    when(collector.getCollectionCount()).thenReturn(3L);
    List<MemoryPoolMXBean> pools = List.of(pool("Tenured Gen", MemoryType.HEAP, null));
    GcStateDescriber describer = new GcStateDescriber(List.of(collector), pools, () -> UPTIME_MS);

    assertEquals(describer.describe(), "oldGenUsedAfterLastGcBytes: -1, collectors: [Copy (count: 3)]");
  }

  @Test
  public void testDescribeNeverFails() {
    GarbageCollectorMXBean collector = mock(GarbageCollectorMXBean.class);
    when(collector.getName()).thenThrow(new IllegalStateException("bean unregistered"));
    GcStateDescriber describer = new GcStateDescriber(List.of(collector), List.of(), () -> UPTIME_MS);

    assertEquals(describer.describe(), "unavailable: java.lang.IllegalStateException: bean unregistered");
  }

  @Test
  public void testFindOldGenPools() {
    // G1
    assertEquals(oldGenPoolNames(pool("G1 Eden Space", MemoryType.HEAP, null),
        pool("G1 Survivor Space", MemoryType.HEAP, null), pool("G1 Old Gen", MemoryType.HEAP, null),
        pool("Metaspace", MemoryType.NON_HEAP, null)), List.of("G1 Old Gen"));
    // Serial
    assertEquals(oldGenPoolNames(pool("Eden Space", MemoryType.HEAP, null),
        pool("Survivor Space", MemoryType.HEAP, null), pool("Tenured Gen", MemoryType.HEAP, null)),
        List.of("Tenured Gen"));
    // Generational ZGC
    assertEquals(oldGenPoolNames(pool("ZGC Young Generation", MemoryType.HEAP, null),
        pool("ZGC Old Generation", MemoryType.HEAP, null)), List.of("ZGC Old Generation"));
    // Shenandoah has a single heap pool
    assertEquals(oldGenPoolNames(pool("Shenandoah", MemoryType.HEAP, null),
        pool("Metaspace", MemoryType.NON_HEAP, null)), List.of("Shenandoah"));
  }

  private static List<String> oldGenPoolNames(MemoryPoolMXBean... pools) {
    return GcStateDescriber.findOldGenPools(List.of(pools)).stream()
        .map(MemoryPoolMXBean::getName)
        .collect(Collectors.toList());
  }

  private static MemoryPoolMXBean pool(String name, MemoryType type, @Nullable Long collectionUsedBytes) {
    MemoryPoolMXBean pool = mock(MemoryPoolMXBean.class);
    when(pool.getName()).thenReturn(name);
    when(pool.getType()).thenReturn(type);
    if (collectionUsedBytes != null) {
      when(pool.getCollectionUsage()).thenReturn(new MemoryUsage(0, collectionUsedBytes, collectionUsedBytes, -1));
    }
    return pool;
  }

  private static GarbageCollectorMXBean collector(String name, long count, @Nullable Long lastGcEndTimeMs) {
    com.sun.management.GarbageCollectorMXBean collector = mock(com.sun.management.GarbageCollectorMXBean.class);
    when(collector.getName()).thenReturn(name);
    when(collector.getCollectionCount()).thenReturn(count);
    if (lastGcEndTimeMs != null) {
      GcInfo gcInfo = mock(GcInfo.class);
      when(gcInfo.getEndTime()).thenReturn(lastGcEndTimeMs);
      when(collector.getLastGcInfo()).thenReturn(gcInfo);
    }
    return collector;
  }
}
