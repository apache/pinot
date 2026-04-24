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
package org.apache.pinot.controller.helix.core.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class DefaultLineageManagerTest {
  private DefaultLineageManager _lineageManager;

  @BeforeMethod
  public void setUp() {
    _lineageManager = new DefaultLineageManager(Mockito.mock(ControllerConf.class));
  }

  private TableConfigBuilder refreshTableBuilder() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", false));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable_OFFLINE")
        .setIngestionConfig(ingestionConfig);
  }

  private TableConfigBuilder appendTableBuilder() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "DAILY", false));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable_OFFLINE")
        .setIngestionConfig(ingestionConfig);
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip: verify field names deserialize correctly
  // ---------------------------------------------------------------------------

  @Test
  public void testNewFieldsRoundTripSerialization()
      throws Exception {
    TableConfig original = refreshTableBuilder()
        .setReplacedSegmentsRetentionPeriod("2d")
        .setLineageEntryCleanupRetentionPeriod("12h")
        .build();

    String json = JsonUtils.objectToString(original);
    TableConfig deserialized = JsonUtils.stringToObject(json, TableConfig.class);

    assertEquals(deserialized.getValidationConfig().getReplacedSegmentsRetentionPeriod(), "2d");
    assertEquals(deserialized.getValidationConfig().getLineageEntryCleanupRetentionPeriod(), "12h");
  }

  @Test
  public void testNewFieldsDefaultToNullWhenAbsent()
      throws Exception {
    TableConfig original = refreshTableBuilder().build();

    String json = JsonUtils.objectToString(original);
    TableConfig deserialized = JsonUtils.stringToObject(json, TableConfig.class);

    assertNull(deserialized.getValidationConfig().getReplacedSegmentsRetentionPeriod());
    assertNull(deserialized.getValidationConfig().getLineageEntryCleanupRetentionPeriod());
  }

  // ---------------------------------------------------------------------------
  // shouldDeleteReplacedSegments / COMPLETED lineage
  // ---------------------------------------------------------------------------

  @Test
  public void testRefreshTableDefaultRetentionDoesNotDeleteRecentlyCompletedLineage() {
    // REFRESH table with default 1-day retention: lineage just completed → segments must NOT be deleted
    TableConfig tableConfig = refreshTableBuilder().build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis();
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Arrays.asList("src1"), Arrays.asList("dst1"), LineageEntryState.COMPLETED, recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("src1", "dst1"), segmentsToDelete,
        new HashSet<>());

    assertFalse(segmentsToDelete.contains("src1"),
        "Source segment must not be deleted within the default 1-day replaced-segments retention");
  }

  @Test
  public void testRefreshTableDefaultRetentionDeletesOldCompletedLineage() {
    // REFRESH table with default 1-day retention: lineage completed >1 day ago → segments MUST be deleted
    TableConfig tableConfig = refreshTableBuilder().build();

    String entryId = UUID.randomUUID().toString();
    long oldTimestamp = System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000L; // 2 days ago
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Arrays.asList("src1"), Arrays.asList("dst1"), LineageEntryState.COMPLETED, oldTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("src1", "dst1"), segmentsToDelete,
        new HashSet<>());

    assertTrue(segmentsToDelete.contains("src1"),
        "Source segment must be deleted after the default 1-day replaced-segments retention has elapsed");
  }

  @Test
  public void testRefreshTableZeroRetentionDeletesImmediately() {
    // REFRESH table with replacedSegmentsRetentionPeriod="0d" → delete immediately after COMPLETED
    TableConfig tableConfig = refreshTableBuilder().setReplacedSegmentsRetentionPeriod("0d").build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis() - 1; // 1ms in the past to satisfy strict < with 0 retention
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Arrays.asList("src1"), Arrays.asList("dst1"), LineageEntryState.COMPLETED, recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("src1", "dst1"), segmentsToDelete,
        new HashSet<>());

    assertTrue(segmentsToDelete.contains("src1"),
        "Source segment must be deleted immediately when replacedSegmentsRetentionPeriod is 0d");
  }

  @Test
  public void testAppendTableAlwaysDeletesRegardlessOfRetentionConfig() {
    // APPEND table: source segments are always eligible for deletion regardless of retention setting
    TableConfig tableConfig = appendTableBuilder().setReplacedSegmentsRetentionPeriod("7d").build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis();
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Arrays.asList("src1"), Arrays.asList("dst1"), LineageEntryState.COMPLETED, recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("src1", "dst1"), segmentsToDelete,
        new HashSet<>());

    assertTrue(segmentsToDelete.contains("src1"),
        "APPEND table source segments must always be eligible for deletion");
  }

  // ---------------------------------------------------------------------------
  // lineageEntryCleanupRetentionPeriod / IN_PROGRESS lineage
  // ---------------------------------------------------------------------------

  @Test
  public void testDefaultRetentionDoesNotCleanupRecentInProgressLineage() {
    // Default 1-day cleanup retention: IN_PROGRESS entry created recently → must NOT be cleaned up
    TableConfig tableConfig = refreshTableBuilder().build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis();
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Collections.emptyList(), Arrays.asList("dst1"), LineageEntryState.IN_PROGRESS,
            recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("dst1"), segmentsToDelete,
        new HashSet<>());

    assertFalse(segmentsToDelete.contains("dst1"),
        "Destination segment must not be deleted for a recent IN_PROGRESS lineage entry with default retention");
  }

  @Test
  public void testDefaultRetentionCleansUpOldInProgressLineage() {
    // Default 1-day cleanup retention: IN_PROGRESS entry >1 day old → destination segments MUST be deleted
    TableConfig tableConfig = refreshTableBuilder().build();

    String entryId = UUID.randomUUID().toString();
    long oldTimestamp = System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000L; // 2 days ago
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Collections.emptyList(), Arrays.asList("dst1"), LineageEntryState.IN_PROGRESS, oldTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("dst1"), segmentsToDelete,
        new HashSet<>());

    assertTrue(segmentsToDelete.contains("dst1"),
        "Destination segment must be deleted for an old IN_PROGRESS lineage entry with default retention");
  }

  @Test
  public void testZeroLineageCleanupRetentionCleansUpImmediately() {
    // lineageEntryCleanupRetentionPeriod="0d" → IN_PROGRESS entry created just before now is cleaned up
    TableConfig tableConfig = refreshTableBuilder().setLineageEntryCleanupRetentionPeriod("0d").build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis() - 1; // 1ms in the past to satisfy strict < with 0 retention
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Collections.emptyList(), Arrays.asList("dst1"), LineageEntryState.IN_PROGRESS,
            recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("dst1"), segmentsToDelete,
        new HashSet<>());

    assertTrue(segmentsToDelete.contains("dst1"),
        "Destination segment must be deleted immediately when lineageEntryCleanupRetentionPeriod is 0d");
  }

  // ---------------------------------------------------------------------------
  // Unparseable retention period falls back to 1-day default
  // ---------------------------------------------------------------------------

  @Test
  public void testUnparseableReplacedRetentionFallsBackToDefault() {
    // An invalid period string should fall back to the 1-day default, so a recently-completed entry must NOT delete
    TableConfig tableConfig = refreshTableBuilder().setReplacedSegmentsRetentionPeriod("foo").build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis();
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Arrays.asList("src1"), Arrays.asList("dst1"), LineageEntryState.COMPLETED, recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("src1", "dst1"), segmentsToDelete,
        new HashSet<>());

    assertFalse(segmentsToDelete.contains("src1"),
        "Unparseable replacedSegmentsRetentionPeriod must fall back to 1-day default");
  }

  @Test
  public void testUnparseableLineageCleanupRetentionFallsBackToDefault() {
    // An invalid period string should fall back to the 1-day default, so a recent IN_PROGRESS entry must NOT delete
    TableConfig tableConfig = refreshTableBuilder().setLineageEntryCleanupRetentionPeriod("notaperiod").build();

    String entryId = UUID.randomUUID().toString();
    long recentTimestamp = System.currentTimeMillis();
    SegmentLineage lineage = new SegmentLineage("testTable_OFFLINE");
    lineage.addLineageEntry(entryId,
        new LineageEntry(Collections.emptyList(), Arrays.asList("dst1"), LineageEntryState.IN_PROGRESS,
            recentTimestamp));

    List<String> segmentsToDelete = new ArrayList<>();
    _lineageManager.updateLineageForRetention(tableConfig, lineage, Arrays.asList("dst1"), segmentsToDelete,
        new HashSet<>());

    assertFalse(segmentsToDelete.contains("dst1"),
        "Unparseable lineageEntryCleanupRetentionPeriod must fall back to 1-day default");
  }
}
