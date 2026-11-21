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

import com.google.common.annotations.VisibleForTesting;
import com.sun.management.GcInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Locale;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/// Describes the JVM garbage collection state for the heap-based query kill logs.
///
/// The used heap that the accountant compares against its thresholds includes garbage that the next collection will
/// free. The description adds what is needed to tell a heap full of live data from a heap full of garbage:
/// - The old generation usage after the last collection, which is the best cheap estimate of the live set. For G1 the
///   JVM updates it after every collection, young ones included.
/// - For each collector, the number of collections and how long ago the last one ended.
///
/// The class keeps no state. It reads the MXBeans only when [#describe()] is called, which happens on level
/// transitions and kills, not on every sample.
public class GcStateDescriber {
  // Heap pool names that identify the old generation: "G1 Old Gen", "PS Old Gen", "Tenured Gen",
  // "ZGC Old Generation"
  private static final List<String> OLD_POOL_MARKERS = List.of("old", "tenured");
  // Heap pool names that identify the young generation, used when no pool matches the old markers
  private static final List<String> YOUNG_POOL_MARKERS = List.of("eden", "survivor", "young", "nursery");
  // Declared after the markers, which the constructor reads
  private static final GcStateDescriber INSTANCE = new GcStateDescriber(ManagementFactory.getGarbageCollectorMXBeans(),
      ManagementFactory.getMemoryPoolMXBeans(), ManagementFactory.getRuntimeMXBean()::getUptime);

  private final List<GarbageCollectorMXBean> _collectors;
  private final List<MemoryPoolMXBean> _oldGenPools;
  private final LongSupplier _uptimeMs;

  @VisibleForTesting
  GcStateDescriber(List<GarbageCollectorMXBean> collectors, List<MemoryPoolMXBean> pools, LongSupplier uptimeMs) {
    _collectors = collectors;
    _oldGenPools = findOldGenPools(pools);
    _uptimeMs = uptimeMs;
  }

  public static GcStateDescriber getInstance() {
    return INSTANCE;
  }

  /// Returns a one-line description of the GC state, for example:
  /// `oldGenUsedAfterLastGcBytes: 49392123904, collectors: [G1 Young Generation (count: 1644482, last ended 120 ms
  /// ago), G1 Concurrent GC (count: 3042472, last ended 1300 ms ago), G1 Old Generation (count: 154, last ended
  /// 3400 ms ago)]`
  public String describe() {
    try {
      return "oldGenUsedAfterLastGcBytes: " + getOldGenUsedAfterLastGcBytes() + ", collectors: "
          + _collectors.stream().map(this::describe).collect(Collectors.joining(", ", "[", "]"));
    } catch (RuntimeException e) {
      // The description is only diagnostic. Never let it break the kill path.
      return "unavailable: " + e;
    }
  }

  /// Returns the old generation usage right after the last collection, or -1 when the JVM does not report it.
  @VisibleForTesting
  long getOldGenUsedAfterLastGcBytes() {
    long usedBytes = 0;
    boolean reported = false;
    for (MemoryPoolMXBean pool : _oldGenPools) {
      MemoryUsage collectionUsage = pool.getCollectionUsage();
      if (collectionUsage != null) {
        usedBytes += collectionUsage.getUsed();
        reported = true;
      }
    }
    return reported ? usedBytes : -1;
  }

  private String describe(GarbageCollectorMXBean collector) {
    String description = collector.getName() + " (count: " + collector.getCollectionCount();
    Long lastEndTimeMs = getLastGcEndTimeMs(collector);
    if (lastEndTimeMs != null) {
      description += ", last ended " + (_uptimeMs.getAsLong() - lastEndTimeMs) + " ms ago";
    }
    return description + ")";
  }

  /// Returns the end time of the last collection in ms since the JVM start, or null if the collector did not run yet
  /// or the JVM does not expose it.
  @Nullable
  private static Long getLastGcEndTimeMs(GarbageCollectorMXBean collector) {
    if (!(collector instanceof com.sun.management.GarbageCollectorMXBean)) {
      return null;
    }
    GcInfo lastGcInfo = ((com.sun.management.GarbageCollectorMXBean) collector).getLastGcInfo();
    return lastGcInfo != null ? lastGcInfo.getEndTime() : null;
  }

  /// Returns the heap pools that hold the old generation. A collector with a single heap pool (e.g. Shenandoah) has no
  /// old generation pool, so the fallback is every heap pool that is not a young generation pool.
  @VisibleForTesting
  static List<MemoryPoolMXBean> findOldGenPools(List<MemoryPoolMXBean> pools) {
    List<MemoryPoolMXBean> heapPools =
        pools.stream().filter(pool -> pool.getType() == MemoryType.HEAP).collect(Collectors.toList());
    List<MemoryPoolMXBean> oldPools =
        heapPools.stream().filter(pool -> containsAny(pool.getName(), OLD_POOL_MARKERS)).collect(Collectors.toList());
    if (!oldPools.isEmpty()) {
      return oldPools;
    }
    return heapPools.stream()
        .filter(pool -> !containsAny(pool.getName(), YOUNG_POOL_MARKERS))
        .collect(Collectors.toList());
  }

  private static boolean containsAny(String name, List<String> markers) {
    String lowerCaseName = name.toLowerCase(Locale.ROOT);
    return markers.stream().anyMatch(lowerCaseName::contains);
  }
}
