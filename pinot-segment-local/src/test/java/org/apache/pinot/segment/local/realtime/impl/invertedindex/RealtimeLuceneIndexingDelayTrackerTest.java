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

import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;


public class RealtimeLuceneIndexingDelayTrackerTest {
  private RealtimeLuceneIndexingDelayTracker _realtimeLuceneIndexingDelayTracker;
  private ServerMetrics _serverMetrics;

  @BeforeMethod
  public void setUp() {
    _serverMetrics = mock(ServerMetrics.class);
    ServerMetrics.deregister();
    ServerMetrics.register(_serverMetrics);
    _realtimeLuceneIndexingDelayTracker = RealtimeLuceneIndexingDelayTracker.getInstance();
    _realtimeLuceneIndexingDelayTracker.reset();
  }

  @Test
  public void testRegistersGaugesPerTable() {
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment1", "column1", 1, () -> 0, () -> 0L);
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table2", "segment1", "column1", 1, () -> 0, () -> 0L);

    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_DOCS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_MS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table2"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_DOCS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table2"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_MS),
        Mockito.any());
  }

  @Test
  public void testRegistersGaugesPerPartition() {
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment1", "column1", 1, () -> 0, () -> 0L);
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment1", "column1", 2, () -> 0, () -> 0L);

    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_DOCS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(1), eq(ServerGauge.LUCENE_INDEXING_DELAY_MS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(2), eq(ServerGauge.LUCENE_INDEXING_DELAY_DOCS),
        Mockito.any());
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq("table1"), eq(2), eq(ServerGauge.LUCENE_INDEXING_DELAY_MS),
        Mockito.any());
  }

  @Test
  public void testCleansUpGauges() {
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment1", "column1", 1, () -> 0, () -> 0L);
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment2", "column1", 1, () -> 0, () -> 0L);

    // should not call remove if there are still other segments for the table and partition
    _realtimeLuceneIndexingDelayTracker.clear("table1", "segment1", "column1", 1);
    verify(_serverMetrics, times(0)).removePartitionGauge("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_MS);
    verify(_serverMetrics, times(0)).removePartitionGauge("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_MS);

    _realtimeLuceneIndexingDelayTracker.clear("table1", "segment2", "column1", 1);
    // if the last segment is removed, the partition gauge should be removed as well
    verify(_serverMetrics).removePartitionGauge("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_DOCS);
    verify(_serverMetrics).removePartitionGauge("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_MS);
  }

  @Test
  public void testEmitsMaxDelayPerPartition() {
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment1", "column1", 1, () -> 10, () -> 20L);
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table1", "segment2", "column1", 1, () -> 5, () -> 15L);
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers("table2", "segment1", "column1", 1, () -> 25, () -> 30L);

    verifyGaugeValue("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_DOCS, 10);
    verifyGaugeValue("table1", 1, ServerGauge.LUCENE_INDEXING_DELAY_MS, 20);
    verifyGaugeValue("table2", 1, ServerGauge.LUCENE_INDEXING_DELAY_DOCS, 25);
    verifyGaugeValue("table2", 1, ServerGauge.LUCENE_INDEXING_DELAY_MS, 30);
  }

  /**
   * Helper method to verify the value of a gauge. If the gauge's supplier is updated multiple times, only
   * the last value will be verified.
   */
  private void verifyGaugeValue(String table, int partition, ServerGauge gauge, long expectedValue) {
    ArgumentCaptor<Supplier<Long>> gaugeSupplierCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(_serverMetrics, atLeastOnce()).setOrUpdatePartitionGauge(eq(table), eq(partition), eq(gauge),
        gaugeSupplierCaptor.capture());
    assertEquals(gaugeSupplierCaptor.getValue().get(), expectedValue, "unexpected reported value for gauge");
  }
}
