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
package org.apache.pinot.perf;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator.IntGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapBytesGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapDoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapGroupByUtils;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapIntGroupIdMap;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Deterministic memory-footprint and throughput measurement (not a JMH benchmark): builds the per-segment
/// group-by state — one key table plus two result holders, mirroring a `GROUP BY k ... COUNT(*), SUM(m)` —
/// at the requested group counts, forces GC, and reports:
/// <ul>
///   <li>retained JVM heap vs direct (off-heap) memory while the state is live (the bytes `groupByOffHeap`
///   moves off the heap),</li>
///   <li>build time (one insert per distinct key), lookup time (a full second all-hits pass), and the GC time
///   accumulated during the build.</li>
/// </ul>
/// Keys are generated on the fly, so only state genuinely retained by the structures is measured (the on-heap
/// string map retains the String keys, exactly like the on-heap no-dictionary generator does).
///
/// Usage: {@code java -XmxSIZE -XX:MaxDirectMemorySize=SIZE -cp benchmarks.jar
/// org.apache.pinot.perf.OffHeapGroupByMemoryFootprint [int|string|all] [count...]}
/// (defaults: all 100_000 1_000_000 4_000_000). At 100M groups use ~12GB heap for the int tier and ~20GB heap for
/// the on-heap string tier (100M retained Strings), with MaxDirectMemorySize of at least 10GB.
public final class OffHeapGroupByMemoryFootprint {
  private OffHeapGroupByMemoryFootprint() {
  }

  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  public static void main(String[] args)
      throws Exception {
    String tier = args.length > 0 ? args[0] : "all";
    int[] groupCounts;
    if (args.length > 1) {
      groupCounts = new int[args.length - 1];
      for (int i = 1; i < args.length; i++) {
        groupCounts[i - 1] = Integer.parseInt(args[i].replace("_", ""));
      }
    } else {
      groupCounts = new int[]{100_000, 1_000_000, 4_000_000};
    }
    System.out.printf("%-24s %12s %11s %11s %10s %10s %12s%n",
        "configuration", "numGroups", "heap MB", "direct MB", "build ms", "lookup ms", "gc build ms");
    for (int numGroups : groupCounts) {
      if (!"string".equals(tier)) {
        measure("onHeap int tier", numGroups, new OnHeapIntState(numGroups));
        measure("offHeap int tier", numGroups, new OffHeapIntState(numGroups));
      }
      if (!"int".equals(tier)) {
        measure("onHeap string tier", numGroups, new OnHeapStringState(numGroups));
        measure("offHeap string tier", numGroups, new OffHeapStringState(numGroups));
      }
      System.out.println();
    }
  }

  /// One measured configuration: build inserts every distinct key once, lookup re-resolves every key (all hits).
  private abstract static class TierState {
    final int _numGroups;
    final List<AutoCloseable> _closeables = new ArrayList<>();

    TierState(int numGroups) {
      _numGroups = numGroups;
    }

    abstract void build();

    abstract void lookup();

    void close()
        throws Exception {
      for (AutoCloseable closeable : _closeables) {
        closeable.close();
      }
      _closeables.clear();
    }

    GroupByResultHolder buildHolder(boolean offHeap) {
      GroupByResultHolder holder = offHeap
          ? new OffHeapDoubleGroupByResultHolder(Math.min(_numGroups, 10_000), _numGroups, 0.0)
          : new DoubleGroupByResultHolder(Math.min(_numGroups, 10_000), _numGroups, 0.0);
      if (holder instanceof AutoCloseable) {
        _closeables.add((AutoCloseable) holder);
      }
      holder.ensureCapacity(_numGroups);
      return holder;
    }
  }

  private static final class OnHeapIntState extends TierState {
    private IntGroupIdMap _map;
    private GroupByResultHolder _holder1;
    private GroupByResultHolder _holder2;

    OnHeapIntState(int numGroups) {
      super(numGroups);
    }

    @Override
    void build() {
      _map = new IntGroupIdMap();
      for (int i = 0; i < _numGroups; i++) {
        _map.getGroupId(i * 31, Integer.MAX_VALUE);
      }
      _holder1 = buildHolder(false);
      _holder2 = buildHolder(false);
    }

    @Override
    void lookup() {
      long sum = 0;
      for (int i = 0; i < _numGroups; i++) {
        sum += _map.getGroupId(i * 31, Integer.MAX_VALUE);
      }
      consume(sum);
    }
  }

  private static final class OffHeapIntState extends TierState {
    private OffHeapIntGroupIdMap _map;
    private GroupByResultHolder _holder1;
    private GroupByResultHolder _holder2;

    OffHeapIntState(int numGroups) {
      super(numGroups);
    }

    @Override
    void build() {
      _map = new OffHeapIntGroupIdMap(0);
      _closeables.add(_map);
      for (int i = 0; i < _numGroups; i++) {
        _map.getGroupId(i * 31, Integer.MAX_VALUE);
      }
      _holder1 = buildHolder(true);
      _holder2 = buildHolder(true);
    }

    @Override
    void lookup() {
      long sum = 0;
      for (int i = 0; i < _numGroups; i++) {
        sum += _map.getGroupId(i * 31, Integer.MAX_VALUE);
      }
      consume(sum);
    }
  }

  private static final class OnHeapStringState extends TierState {
    private Object2IntOpenHashMap<String> _map;
    private GroupByResultHolder _holder1;
    private GroupByResultHolder _holder2;

    OnHeapStringState(int numGroups) {
      super(numGroups);
    }

    @Override
    void build() {
      _map = new Object2IntOpenHashMap<>();
      _map.defaultReturnValue(-1);
      for (int i = 0; i < _numGroups; i++) {
        // The map retains the String keys, exactly like the on-heap no-dictionary generator does
        _map.putIfAbsent(makeKey(i), _map.size());
      }
      _holder1 = buildHolder(false);
      _holder2 = buildHolder(false);
    }

    @Override
    void lookup() {
      long sum = 0;
      for (int i = 0; i < _numGroups; i++) {
        // Fresh String per lookup, mirroring per-block string materialization
        sum += _map.getInt(makeKey(i));
      }
      consume(sum);
    }
  }

  private static final class OffHeapStringState extends TierState {
    private OffHeapBytesGroupIdMap _map;
    private final byte[] _scratch = new byte[128];
    private GroupByResultHolder _holder1;
    private GroupByResultHolder _holder2;

    OffHeapStringState(int numGroups) {
      super(numGroups);
    }

    @Override
    void build() {
      _map = new OffHeapBytesGroupIdMap(0);
      _closeables.add(_map);
      for (int i = 0; i < _numGroups; i++) {
        int length = OffHeapGroupByUtils.encodeUtf8(makeKey(i), _scratch);
        _map.getGroupId(_scratch, 0, length, Integer.MAX_VALUE);
      }
      _holder1 = buildHolder(true);
      _holder2 = buildHolder(true);
    }

    @Override
    void lookup() {
      long sum = 0;
      for (int i = 0; i < _numGroups; i++) {
        int length = OffHeapGroupByUtils.encodeUtf8(makeKey(i), _scratch);
        sum += _map.getGroupId(_scratch, 0, length, Integer.MAX_VALUE);
      }
      consume(sum);
    }
  }

  /// Fast distinct-key builder (String.format is ~1us/call, far too slow for 100M keys): "key-" + 9 digits +
  /// "-abcdefgh", 22 chars, a fresh String per call like per-block string materialization.
  private static String makeKey(int i) {
    char[] chars = {'k', 'e', 'y', '-', '0', '0', '0', '0', '0', '0', '0', '0', '0', '-',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    int value = i;
    for (int position = 12; position >= 4 && value > 0; position--) {
      chars[position] = (char) ('0' + (value % 10));
      value /= 10;
    }
    return new String(chars);
  }

  private static volatile long _sink;

  private static void consume(long value) {
    _sink = value;
  }

  private static void measure(String label, int numGroups, TierState state)
      throws Exception {
    forceGc();
    long heapBefore = MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    long directBefore = PinotDataBuffer.getDirectBufferUsage();
    long gcBefore = totalGcTimeMs();

    long buildStartNs = System.nanoTime();
    state.build();
    long buildMs = (System.nanoTime() - buildStartNs) / 1_000_000;
    long gcBuildMs = totalGcTimeMs() - gcBefore;

    long lookupStartNs = System.nanoTime();
    state.lookup();
    long lookupMs = (System.nanoTime() - lookupStartNs) / 1_000_000;

    forceGc();
    double heapMb = (MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() - heapBefore) / 1048576.0;
    double directMb = (PinotDataBuffer.getDirectBufferUsage() - directBefore) / 1048576.0;
    System.out.printf("%-24s %,12d %11.1f %11.1f %,10d %,10d %,12d%n",
        label, numGroups, heapMb, directMb, buildMs, lookupMs, gcBuildMs);
    state.close();
  }

  private static long totalGcTimeMs() {
    long total = 0;
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += Math.max(0, gcBean.getCollectionTime());
    }
    return total;
  }

  private static void forceGc()
      throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      System.gc();
      Thread.sleep(100);
    }
  }
}
