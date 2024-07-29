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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
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


public class SegmentLineageCleanupTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String OFFLINE_TABLE_NAME = "segmentTable_OFFLINE";
  private static final String REFRESH_OFFLINE_TABLE_NAME = "refreshSegmentTable_OFFLINE";

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
    _retentionManager = new RetentionManager(_resourceManager, mock(LeadControllerManager.class), controllerConf,
        mock(ControllerMetrics.class));

    // Create a schema
    TEST_INSTANCE.addDummySchema(TableNameBuilder.extractRawTableName(OFFLINE_TABLE_NAME));
    TEST_INSTANCE.addDummySchema(TableNameBuilder.extractRawTableName(REFRESH_OFFLINE_TABLE_NAME));

    // Update table config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(1).build();
    _resourceManager.addTable(tableConfig);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig refreshTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(REFRESH_OFFLINE_TABLE_NAME).setNumReplicas(1)
            .setIngestionConfig(ingestionConfig).build();
    _resourceManager.addTable(refreshTableConfig);
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
  }

  private void verifySegmentsDeleted(int expectedNumRemainingSegments) {
    // Segment deletion happens asynchronously
    TestUtils.waitForCondition(aVoid -> getNumSegments(OFFLINE_TABLE_NAME) == expectedNumRemainingSegments, 60_000L,
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

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
