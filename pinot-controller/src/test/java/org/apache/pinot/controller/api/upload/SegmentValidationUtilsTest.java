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
package org.apache.pinot.controller.api.upload;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


public class SegmentValidationUtilsTest {

  private static SegmentMetadata mockMetadata(long endTimeMs, long indexCreationTimeMs) {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeInterval()).thenReturn(null);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Mockito.when(metadata.getEndTime()).thenReturn(endTimeMs);
    Mockito.when(metadata.getIndexCreationTime()).thenReturn(indexCreationTimeMs);
    Mockito.when(metadata.getName()).thenReturn("seg");
    return metadata;
  }

  @Test
  public void rejectUploadSkippedWhenDisabled() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
        tableConfig, now, false, null);
  }

  @Test
  public void rejectUploadSkippedForOfflineRefresh() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .setSegmentPushType("REFRESH")
        .build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
        tableConfig, now, true, null);
  }

  @Test
  public void rejectUploadSkippedWhenRetentionInvalid() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("BAD")
        .setRetentionTimeValue("7")
        .build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
        tableConfig, now, true, null);
  }

  @Test
  public void rejectUploadWhenEnabledAndSegmentOutOfRetention() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    ControllerMetrics controllerMetrics = Mockito.mock(ControllerMetrics.class);
    try {
      SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
          tableConfig, now, true, controllerMetrics);
      Assert.fail("expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 403);
    }
    Mockito.verify(controllerMetrics).addMeteredGlobalValue(ControllerMeter.OUT_OF_RETENTION_SEGMENT_UPLOAD_REJECTED,
        1L);
  }

  @Test
  public void rejectUploadAllowsRecentSegment() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(1), 0L),
        tableConfig, now, true, null);
  }

  @Test
  public void rejectUploadDoesNotUseIndexCreationTimeFallback() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(-1, now - TimeUnit.DAYS.toMillis(30)),
        tableConfig, now, true, null);
  }

  @Test
  public void rejectUploadSkippedWhenValidationConfigNull() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = Mockito.mock(TableConfig.class);
    Mockito.when(tableConfig.getValidationConfig()).thenReturn(null);
    ControllerMetrics controllerMetrics = Mockito.mock(ControllerMetrics.class);
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
        tableConfig, now, true, controllerMetrics);
    verify(controllerMetrics, never()).addMeteredGlobalValue(any(ControllerMeter.class), anyLong());
  }

  @Test
  public void rejectUploadSkippedWhenRetentionNotConfigured() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE").build();
    SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
        tableConfig, now, true, null);
  }

  @Test
  public void rejectUploadThrowsWhenOutOfRetentionAndMetricsNull() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    try {
      SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
          tableConfig, now, true, null);
      Assert.fail("expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 403);
    }
  }

  @Test
  public void rejectUploadRealtimeOutOfRetentionThrows() {
    long now = System.currentTimeMillis();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("t_REALTIME")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();
    try {
      SegmentValidationUtils.rejectUploadIfOutOfRetention(mockMetadata(now - TimeUnit.DAYS.toMillis(30), 0L),
          tableConfig, now, true, null);
      Assert.fail("expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 403);
    }
  }
}
