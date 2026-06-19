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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.BrokerServiceHelper;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentLineageCleanupTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String OFFLINE_TABLE_NAME = "segmentTable_OFFLINE";
  private static final String REFRESH_OFFLINE_TABLE_NAME = "refreshSegmentTable_OFFLINE";
  private static final String RETENTION_WINDOW_OFFLINE_TABLE_NAME = "retentionWindowSegmentTable_OFFLINE";
  // Short retention so the test can observe the before/after states without holding up CI for long.
  private static final String RETENTION_WINDOW_REPLACED_SEGMENTS_RETENTION = "2s";
  private static final long RETENTION_WINDOW_REPLACED_SEGMENTS_RETENTION_MS = 2_000L;

  private PinotHelixResourceManager _resourceManager;
  private RetentionManager _retentionManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    _resourceManager = TEST_INSTANCE.getHelixResourceManager();

    // Create the retention manager
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setRetentionControllerFrequencyInSeconds(0);
    controllerConf.setDeletedSegmentsRetentionInDays(0);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(_resourceManager, controllerConf, null, null);
    _retentionManager = new RetentionManager(_resourceManager, mock(LeadControllerManager.class), controllerConf,
        mock(ControllerMetrics.class), brokerServiceHelper);

    // Create a schema
    TEST_INSTANCE.addDummySchema(TableNameBuilder.extractRawTableName(OFFLINE_TABLE_NAME));
    TEST_INSTANCE.addDummySchema(TableNameBuilder.extractRawTableName(REFRESH_OFFLINE_TABLE_NAME));
    TEST_INSTANCE.addDummySchema(TableNameBuilder.extractRawTableName(RETENTION_WINDOW_OFFLINE_TABLE_NAME));

    // Update table config. replacedSegmentsRetentionPeriod is set to 0s so the lineage-cleanup pass deletes
    // replaced segments as soon as the lineage entry timestamp is strictly older than "now". Without this,
    // the default retention (4 hours for this APPEND table) would make the COMPLETED-lineage assertions in
    // testSegmentLineageCleanup unable to observe deletion within the test's wait window.
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(1)
            .setReplacedSegmentsRetentionPeriod("0s").build();
    _resourceManager.addTable(tableConfig);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig refreshTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(REFRESH_OFFLINE_TABLE_NAME).setNumReplicas(1)
            .setIngestionConfig(ingestionConfig).build();
    _resourceManager.addTable(refreshTableConfig);

    // Table dedicated to exercising the replacedSegmentsRetentionPeriod window: the first retention pass must keep
    // the replaced segments, and the second pass (after the window elapses) must delete them.
    TableConfig retentionWindowTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RETENTION_WINDOW_OFFLINE_TABLE_NAME).setNumReplicas(1)
            .setReplacedSegmentsRetentionPeriod(RETENTION_WINDOW_REPLACED_SEGMENTS_RETENTION).build();
    _resourceManager.addTable(retentionWindowTableConfig);
  }

  @Test
  public void testSegmentLineageCleanup() {
    // Create metadata for original segments.
    for (int i = 0; i < 5; i++) {
      _resourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "segment_" + i), "downloadUrl");
    }

    // Create metadata for merged segments.
    for (int i = 0; i < 2; i++) {
      _resourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_" + i), "downloadUrl");
    }

    assertEquals(getNumSegments(OFFLINE_TABLE_NAME), 7);

    // Validate the case when the lineage entry state is 'IN_PROGRESS'.
    long currentTimeMs = System.currentTimeMillis();
    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry("0",
        new LineageEntry(Arrays.asList("segment_0", "segment_1"), Arrays.asList("merged_0"),
            LineageEntryState.IN_PROGRESS, currentTimeMs));
    SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    verifySegmentsDeleted(7);

    // Validate the case when the lineage entry state is 'COMPLETED'.
    segmentLineage.updateLineageEntry("0",
        new LineageEntry(Arrays.asList("segment_0", "segment_1"), Arrays.asList("merged_0"),
            LineageEntryState.COMPLETED, currentTimeMs));
    SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    verifySegmentsDeleted(5);

    // Validate the case when the lineage entry state is 'COMPLETED' and all segments are deleted.
    _resourceManager.deleteSegment(OFFLINE_TABLE_NAME, "merged_0");
    verifySegmentsDeleted(4);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    verifySegmentsDeleted(4);
    assertEquals(getSegments(OFFLINE_TABLE_NAME), Arrays.asList("merged_1", "segment_2", "segment_3", "segment_4"));
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 0);

    // Validate the case when the lineage entry state is 'IN_PROGRESS' and timestamp is old.
    LineageEntry lineageEntry =
        new LineageEntry(Arrays.asList("segment_2", "segment_3"), Arrays.asList("merged_1", "merged_2"),
            LineageEntryState.IN_PROGRESS, currentTimeMs - TimeUnit.DAYS.toMillis(2L));
    segmentLineage.addLineageEntry("1", lineageEntry);
    SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    verifySegmentsDeleted(3);
    assertEquals(getSegments(OFFLINE_TABLE_NAME), Arrays.asList("segment_2", "segment_3", "segment_4"));
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 1);

    // Validate that reaping a zombie IN_PROGRESS entry also deletes a destination znode that never reached the
    // ideal state. The prior pass kept entry "1" because "merged_1" was still in the ideal state. Plant a
    // property-store znode for "merged_2" — which was never added to the ideal state — to mimic a crash between
    // destination-metadata creation and the ideal-state update. Now neither destination is in the ideal state, so
    // the entry must be reaped and the orphan znode deleted along with it.
    ZKMetadataProvider.setSegmentZKMetadata(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME,
        new SegmentZKMetadata("merged_2"));
    assertTrue(_resourceManager.getSegmentsFromPropertyStore(OFFLINE_TABLE_NAME).contains("merged_2"));
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 0);
    // Segment znode deletion is asynchronous, so wait for the orphan to disappear from the property store.
    TestUtils.waitForCondition(
        aVoid -> !_resourceManager.getSegmentsFromPropertyStore(OFFLINE_TABLE_NAME).contains("merged_2"), 60_000L,
        "Orphan destination znode absent from the ideal state must be reaped when the zombie entry is cleaned up");
  }

  private void verifySegmentsDeleted(int expectedNumRemainingSegments) {
    verifySegmentsDeleted(OFFLINE_TABLE_NAME, expectedNumRemainingSegments);
  }

  private void verifySegmentsDeleted(String tableNameWithType, int expectedNumRemainingSegments) {
    // Segment deletion happens asynchronously
    TestUtils.waitForCondition(aVoid -> getNumSegments(tableNameWithType) == expectedNumRemainingSegments, 60_000L,
        "Failed to delete the segments");
  }

  private List<String> getSegments(String tableNameWithType) {
    return _resourceManager.getSegmentsFor(tableNameWithType, false);
  }

  private int getNumSegments(String tableNameWithType) {
    return getSegments(tableNameWithType).size();
  }

  @Test
  public void testRefreshTableCleanup() {
    // Create metadata for original segments
    for (int i = 0; i < 3; i++) {
      _resourceManager.addNewSegment(REFRESH_OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(REFRESH_OFFLINE_TABLE_NAME, "segment1_" + i), "downloadUrl");
    }

    // Create metadata for new segments.
    for (int i = 0; i < 3; i++) {
      _resourceManager.addNewSegment(REFRESH_OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(REFRESH_OFFLINE_TABLE_NAME, "segment2_" + i), "downloadUrl");
    }

    assertEquals(getNumSegments(REFRESH_OFFLINE_TABLE_NAME), 6);

    // Validate the case when the lineage entry state is 'IN_PROGRESS'
    SegmentLineage segmentLineage = new SegmentLineage(REFRESH_OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry("0", new LineageEntry(Arrays.asList("segment1_0", "segment1_1", "segment1_2"),
        Arrays.asList("segment2_0", "segment2_1", "segment2_2"), LineageEntryState.IN_PROGRESS,
        System.currentTimeMillis()));
    SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(REFRESH_OFFLINE_TABLE_NAME);
    assertEquals(getNumSegments(REFRESH_OFFLINE_TABLE_NAME), 6);
    assertEquals(_resourceManager.getSegmentsFor(REFRESH_OFFLINE_TABLE_NAME, true).size(), 3);
  }

  // Verifies that replacedSegmentsRetentionPeriod gates lineage-cleanup deletion: the first retention pass right
  // after the lineage entry transitions to COMPLETED must preserve the replaced segments, and a later pass run
  // after the configured window elapses must delete them.
  @Test
  public void testReplacedSegmentsRetentionWindow()
      throws InterruptedException {
    for (int i = 0; i < 2; i++) {
      _resourceManager.addNewSegment(RETENTION_WINDOW_OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(RETENTION_WINDOW_OFFLINE_TABLE_NAME, "src_" + i), "downloadUrl");
    }
    _resourceManager.addNewSegment(RETENTION_WINDOW_OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(RETENTION_WINDOW_OFFLINE_TABLE_NAME, "merged_0"), "downloadUrl");
    assertEquals(getNumSegments(RETENTION_WINDOW_OFFLINE_TABLE_NAME), 3);

    long completedAtMs = System.currentTimeMillis();
    SegmentLineage segmentLineage = new SegmentLineage(RETENTION_WINDOW_OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry("0",
        new LineageEntry(Arrays.asList("src_0", "src_1"), Arrays.asList("merged_0"), LineageEntryState.COMPLETED,
            completedAtMs));
    SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), segmentLineage, -1);

    // First pass runs inside the retention window — replaced segments must remain.
    _retentionManager.processTable(RETENTION_WINDOW_OFFLINE_TABLE_NAME);
    assertEquals(getNumSegments(RETENTION_WINDOW_OFFLINE_TABLE_NAME), 3);

    // Sleep just past the configured window. Add a small buffer because the cleanup uses a strict less-than
    // comparison against `now - retentionMs`, and we want any clock granularity to be on the safe side.
    long sleepMs = (System.currentTimeMillis() - completedAtMs > RETENTION_WINDOW_REPLACED_SEGMENTS_RETENTION_MS)
        ? 100L : (RETENTION_WINDOW_REPLACED_SEGMENTS_RETENTION_MS + 200L);
    Thread.sleep(sleepMs);

    // Second pass runs after the window — replaced segments must now be deleted.
    _retentionManager.processTable(RETENTION_WINDOW_OFFLINE_TABLE_NAME);
    verifySegmentsDeleted(RETENTION_WINDOW_OFFLINE_TABLE_NAME, 1);
    assertEquals(getSegments(RETENTION_WINDOW_OFFLINE_TABLE_NAME), Collections.singletonList("merged_0"));
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
