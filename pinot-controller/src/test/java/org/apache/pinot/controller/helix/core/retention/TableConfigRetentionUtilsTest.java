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
package org.apache.pinot.controller.helix.core.retention;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class TableConfigRetentionUtilsTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  @Test
  public void testBuildRetentionStrategyUsesConfiguredWindow() {
    RetentionStrategy strategy = TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("DAYS", "7"), false);
    assertNotNull(strategy);
    long now = System.currentTimeMillis();
    String tableName = RAW_TABLE_NAME + "_OFFLINE";
    assertTrue(strategy.isPurgeable(tableName, SEGMENT_NAME, now - TimeUnit.DAYS.toMillis(8)));
    assertFalse(strategy.isPurgeable(tableName, SEGMENT_NAME, now - TimeUnit.DAYS.toMillis(6)));
  }

  @Test
  public void testBuildRetentionStrategyAcceptsLowercaseUnit() {
    RetentionStrategy strategy = TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("hours", "2"), false);
    assertNotNull(strategy);
    long now = System.currentTimeMillis();
    String tableName = RAW_TABLE_NAME + "_OFFLINE";
    assertTrue(strategy.isPurgeable(tableName, SEGMENT_NAME, now - TimeUnit.HOURS.toMillis(3)));
    assertFalse(strategy.isPurgeable(tableName, SEGMENT_NAME, now - TimeUnit.HOURS.toMillis(1)));
  }

  @Test
  public void testBuildRetentionStrategyPropagatesCreationTimeFallback() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    segmentZKMetadata.setCreationTime(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30));
    String tableName = RAW_TABLE_NAME + "_OFFLINE";

    RetentionStrategy withFallback =
        TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("DAYS", "7"), true);
    assertNotNull(withFallback);
    assertTrue(withFallback.isPurgeable(tableName, segmentZKMetadata));

    RetentionStrategy withoutFallback =
        TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("DAYS", "7"), false);
    assertNotNull(withoutFallback);
    assertFalse(withoutFallback.isPurgeable(tableName, segmentZKMetadata));
  }

  @Test
  public void testBuildRetentionStrategyReturnsNullForInvalidOrMissingRetention() {
    assertNull(TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("BAD", "7"), false));
    assertNull(TableConfigRetentionUtils.buildRetentionStrategy(getTableConfig("DAYS", "abc"), false));
    assertNull(TableConfigRetentionUtils.buildRetentionStrategy(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build(), false));
  }

  private static TableConfig getTableConfig(String retentionTimeUnit, String retentionTimeValue) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setRetentionTimeUnit(retentionTimeUnit)
        .setRetentionTimeValue(retentionTimeValue)
        .build();
  }
}
