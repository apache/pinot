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
package org.apache.pinot.controller.api.resources;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

public class ResourceUtilsTest {

  private static final String RAW_TABLE = "myTable";
  private static final long SEGMENT_BYTES = 123L;

  private ControllerMetrics _metrics;

  /** Reset static Atomics inside ResourceUtils between tests so they don’t bleed values. */
  @BeforeMethod
  public void setUp() throws Exception {
    _metrics = mock(ControllerMetrics.class);
    resetStaticCounter("_deepStoreWriteOpsInProgress");
    resetStaticCounter("_deepStoreWriteBytesInProgress");
    resetStaticCounter("_deepStoreReadOpsInProgress");
    resetStaticCounter("_deepStoreReadBytesInProgress");
  }

  @Test
  public void testUploadMetricsFlow() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    // This test verifies the flow of metrics emitted during a segment upload to deep store.
    ResourceUtils.emitPreSegmentUploadMetrics(_metrics, RAW_TABLE, SEGMENT_BYTES);

    verify(_metrics).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_WRITE_OPS_IN_PROGRESS, 1L);
    verify(_metrics).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_WRITE_BYTES_IN_PROGRESS, SEGMENT_BYTES);

    // Simulate upload latency
    long simulatedDelay = 50L;
    Thread.sleep(simulatedDelay);

    // Post-upload metrics
    ResourceUtils.emitPostSegmentUploadMetrics(_metrics, RAW_TABLE, startTime, SEGMENT_BYTES);

    // ops / bytes gauges should be back to zero
    verify(_metrics, atLeastOnce()).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_WRITE_OPS_IN_PROGRESS, 0L);
    verify(_metrics, atLeastOnce()).setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_WRITE_OPS_IN_PROGRESS, 0L);
    verify(_metrics, atLeastOnce()).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_WRITE_BYTES_IN_PROGRESS, 0L);
    verify(_metrics, atLeastOnce()).setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_WRITE_BYTES_IN_PROGRESS, 0L);

    // timers & meters fire once
    verify(_metrics).addTimedTableValue(eq(RAW_TABLE),
        eq(ControllerTimer.DEEP_STORE_SEGMENT_WRITE_TIME_MS), anyLong(), eq(TimeUnit.MILLISECONDS));
    verify(_metrics).addMeteredTableValue(RAW_TABLE,
        ControllerMeter.DEEP_STORE_WRITE_BYTES_COMPLETED, SEGMENT_BYTES);
    //  ArgumentCaptor to capture the long duration value that gets passed into the addTimedTableValue(...) method.
    //  This allows the test to inspect the actual argument used when the metric was recorded.
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(_metrics).addTimedTableValue(eq(RAW_TABLE),
        eq(ControllerTimer.DEEP_STORE_SEGMENT_WRITE_TIME_MS), durationCaptor.capture(), eq(TimeUnit.MILLISECONDS));
    assertTrue(durationCaptor.getValue() >= simulatedDelay,
        "Expected write latency >= " + simulatedDelay + "ms but got " + durationCaptor.getValue());
  }

  @Test
  public void testDownloadMetricsFlow() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    ResourceUtils.emitPreSegmentDownloadMetrics(_metrics, RAW_TABLE, SEGMENT_BYTES);
    verify(_metrics).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_READ_OPS_IN_PROGRESS, 1L);
    verify(_metrics).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_READ_BYTES_IN_PROGRESS, SEGMENT_BYTES);

    // Simulate download latency
    long simulatedDelay = 50L;
    Thread.sleep(simulatedDelay);
    // Post-download metrics
    ResourceUtils.emitPostSegmentDownloadMetrics(_metrics, RAW_TABLE, startTime, SEGMENT_BYTES);
    verify(_metrics, atLeastOnce()).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_READ_OPS_IN_PROGRESS, 0L);
    verify(_metrics, atLeastOnce()).setOrUpdateTableGauge(RAW_TABLE,
        ControllerGauge.DEEP_STORE_READ_BYTES_IN_PROGRESS, 0L);

    verify(_metrics).addTimedTableValue(eq(RAW_TABLE),
        eq(ControllerTimer.DEEP_STORE_SEGMENT_READ_TIME_MS), anyLong(), eq(TimeUnit.MILLISECONDS));
    verify(_metrics).addMeteredTableValue(RAW_TABLE,
        ControllerMeter.DEEP_STORE_READ_BYTES_COMPLETED, SEGMENT_BYTES);
    // Capture and verify the read latency
    //  ArgumentCaptor to capture the long duration value that gets passed into the addTimedTableValue(...) method.
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(_metrics).addTimedTableValue(eq(RAW_TABLE), eq(ControllerTimer.DEEP_STORE_SEGMENT_READ_TIME_MS),
        durationCaptor.capture(), eq(TimeUnit.MILLISECONDS));
    assertTrue(durationCaptor.getValue() >= simulatedDelay,
        "Expected read latency >= " + simulatedDelay + "ms but got " + durationCaptor.getValue());
  }

  /** resets the private static AtomicLongs inside ResourceUtils. */
  private static void resetStaticCounter(String fieldName) throws Exception {
    Field declaredField = ResourceUtils.class.getDeclaredField(fieldName);
    declaredField.setAccessible(true);
    ((AtomicLong) declaredField.get(null)).set(0L);
  }
}
