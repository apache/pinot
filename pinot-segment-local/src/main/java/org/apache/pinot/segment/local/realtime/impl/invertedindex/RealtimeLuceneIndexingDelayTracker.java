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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;


/**
 * RealtimeLuceneIndexingDelayTracker is used to track the last Lucene refresh time for each segment,
 * and each partition of a table.
 * <p>
 * To reduce maximum metric cardinality, only the maximum delay for a partition is emitted. In practice,
 * segment/column granularity is similar.
 * <p>
 * For example, if a table has 3 segments and 2 partitions, the metrics emitted will be:
 * <p> - luceneIndexDelayMs.table1.partition0
 * <p> - luceneIndexDelayMs.table1.partition1
 * <p> - luceneIndexDelayDocs.table1.partition0
 * <p> - luceneIndexDelayDocs.table1.partition1
 * <p> where the values are the maximum indexDelayMs or indexDelayDocs across all segments for that partition
 */
public class RealtimeLuceneIndexingDelayTracker {
  private final Map<String, TableDelay> _tableToPartitionToDelayMs;
  // Lock is used to prevent removing a gauge while a new suppliers are being registered. Otherwise, it is possible
  // for the gauge to be removed after ServerMetrics.setOrUpdatePartitionGauge() has been called for the next segment
  private final ReentrantLock _lock = new ReentrantLock();

  private RealtimeLuceneIndexingDelayTracker() {
    _tableToPartitionToDelayMs = new HashMap<>();
  }

  /**
   * Returns the singleton instance of the RealtimeLuceneIndexingDelayTracker.
   */
  public static RealtimeLuceneIndexingDelayTracker getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private static class SingletonHolder {
    private static final RealtimeLuceneIndexingDelayTracker INSTANCE = new RealtimeLuceneIndexingDelayTracker();
  }

  public void registerDelaySuppliers(String tableName, String segmentName, String columnName, int partition,
      Supplier<Integer> numDocsDelaySupplier, Supplier<Long> timeMsDelaySupplier) {
    _lock.lock();
    try {
      TableDelay tableDelay = _tableToPartitionToDelayMs.getOrDefault(tableName, new TableDelay(tableName));
      tableDelay.registerDelaySuppliers(segmentName, columnName, partition, numDocsDelaySupplier, timeMsDelaySupplier);
      _tableToPartitionToDelayMs.put(tableName, tableDelay);
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Clears the entry for a given table, segment, and partition.
   * If the entry for the partition is empty, the gauge is removed.
   */
  public void clear(String tableName, String segmentName, String columnName, int partition) {
    _lock.lock();
    try {
      TableDelay tableDelay = _tableToPartitionToDelayMs.get(tableName);
      if (tableDelay != null) {
        tableDelay.clearPartitionDelay(segmentName, columnName, partition);
        if (tableDelay.isEmpty()) {
          _tableToPartitionToDelayMs.remove(tableName);
        }
      }
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Reset the tracker by removing all lastRefreshTimeMs data.
   */
  @VisibleForTesting
  public void reset() {
    _tableToPartitionToDelayMs.clear();
  }

  /**
   * TableDelay is used to track the latest Lucene refresh lastRefreshTimeMs for each partition, of a table.
   */
  private static class TableDelay {
    final String _tableName;
    final Map<Integer, PartitionDelay> _partitionDelayMap;
    final ServerMetrics _serverMetrics;

    TableDelay(String tableName) {
      _tableName = tableName;
      _partitionDelayMap = new HashMap<>();
      _serverMetrics = ServerMetrics.get();
    }

    void registerDelaySuppliers(String segmentName, String columnName, int partition,
        Supplier<Integer> numDocsDelaySupplier, Supplier<Long> timeMsDelaySupplier) {
      PartitionDelay partitionDelay = _partitionDelayMap.getOrDefault(partition, new PartitionDelay(partition));
      partitionDelay.registerDelaySuppliers(segmentName, columnName, numDocsDelaySupplier, timeMsDelaySupplier);
      _partitionDelayMap.put(partition, partitionDelay);
      updateMetrics(partitionDelay);
    }

    /**
     * Clears the entry for a given segment and partition. If the entry for the partition is empty, the
     * gauge is removed. It is important for any stale gauge to be removed as the next segment may not be on the same
     * server.
     */
    void clearPartitionDelay(String segmentName, String columnName, int partition) {
      PartitionDelay partitionDelay = _partitionDelayMap.get(partition);
      if (partitionDelay != null) {
        if (partitionDelay.numEntries() == 1) {
          clearMetrics(partitionDelay);
          _partitionDelayMap.remove(partition);
        }
        partitionDelay.clearSegmentDelay(segmentName, columnName);
      }
    }

    void updateMetrics(PartitionDelay partitionDelay) {
      _serverMetrics.setOrUpdatePartitionGauge(_tableName, partitionDelay._partition,
          ServerGauge.LUCENE_INDEXING_DELAY_MS, partitionDelay::getMaxTimeMsDelay);
      _serverMetrics.setOrUpdatePartitionGauge(_tableName, partitionDelay._partition,
          ServerGauge.LUCENE_INDEXING_DELAY_DOCS, partitionDelay::getMaxNumDocsDelay);
    }

    void clearMetrics(PartitionDelay partitionDelay) {
      _serverMetrics.removePartitionGauge(_tableName, partitionDelay._partition, ServerGauge.LUCENE_INDEXING_DELAY_MS);
      _serverMetrics.removePartitionGauge(_tableName, partitionDelay._partition,
          ServerGauge.LUCENE_INDEXING_DELAY_DOCS);
    }

    boolean isEmpty() {
      return _partitionDelayMap.isEmpty();
    }

    @Override
    public String toString() {
      return "TableDelay{_tableName=" + _tableName + "}";
    }
  }

  /**
   * PartitionDelay is used to track the latest Lucene refresh lastRefreshTimeMs for each segment, of a given partition.
   */
  private static class PartitionDelay {
    final int _partition;
    final Map<String, Supplier<Integer>> _columnNumDocsDelaySuppliers;
    final Map<String, Supplier<Long>> _columnTimeMsDelaySuppliers;

    PartitionDelay(int partition) {
      _partition = partition;
      _columnNumDocsDelaySuppliers = new HashMap<>();
      _columnTimeMsDelaySuppliers = new HashMap<>();
    }

    void registerDelaySuppliers(String segmentName, String columnName, Supplier<Integer> numDocsDelaySupplier,
        Supplier<Long> timeMsDelaySupplier) {
      _columnNumDocsDelaySuppliers.put(getKey(segmentName, columnName), numDocsDelaySupplier);
      _columnTimeMsDelaySuppliers.put(getKey(segmentName, columnName), timeMsDelaySupplier);
    }

    void clearSegmentDelay(String segmentName, String columnName) {
      _columnNumDocsDelaySuppliers.remove(getKey(segmentName, columnName));
      _columnTimeMsDelaySuppliers.remove(getKey(segmentName, columnName));
    }

    long getMaxTimeMsDelay() {
      return _columnTimeMsDelaySuppliers.values().stream().map(Supplier::get).max(Long::compareTo).orElse(-1L);
    }

    long getMaxNumDocsDelay() {
      return _columnNumDocsDelaySuppliers.values().stream().map(Supplier::get).max(Integer::compareTo).orElse(-1);
    }

    int numEntries() {
      return _columnNumDocsDelaySuppliers.size();
    }

    String getKey(String segmentName, String columnName) {
      return segmentName + "." + columnName;
    }

    @Override
    public String toString() {
      return "PartitionDelay{_partition=" + _partition + "}";
    }
  }
}
