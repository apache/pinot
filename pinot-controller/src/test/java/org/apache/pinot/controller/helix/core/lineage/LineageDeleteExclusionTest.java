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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Functional tests for the lineage-aware segment delete check in
 * {@link PinotHelixResourceManager#deleteSegments(String, List)} and the
 * {@code segmentsFrom} re-validation in
 * {@link PinotHelixResourceManager#endReplaceSegments(String, String,
 * org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest)}.
 *
 * <p>Each test uses unique segment names so that the asynchronous segment-ZK-metadata cleanup performed by
 * {@code SegmentDeletionManager} cannot interfere with the next test's segment additions.
 */
public class LineageDeleteExclusionTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String RAW_TABLE_NAME = "lineageDeleteTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private PinotHelixResourceManager _resourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    _resourceManager = TEST_INSTANCE.getHelixResourceManager();
    TEST_INSTANCE.addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(1).build();
    _resourceManager.addTable(tableConfig);
  }

  @BeforeMethod
  public void clearLineage() {
    // Drop any lineage znode written by an earlier test so tests do not bleed into each other.
    SegmentLineageAccessHelper.deleteSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
  }

  private void addSegments(String... segmentNames) {
    for (String name : segmentNames) {
      _resourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, name), "downloadUrl");
    }
  }

  private void writeLineageEntry(String entryId, List<String> segmentsFrom, List<String> segmentsTo,
      LineageEntryState state) {
    SegmentLineage existing =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    SegmentLineage lineage = existing != null ? existing : new SegmentLineage(OFFLINE_TABLE_NAME);
    lineage.addLineageEntry(entryId, new LineageEntry(segmentsFrom, segmentsTo, state, System.currentTimeMillis()));
    assertTrue(SegmentLineageAccessHelper.writeSegmentLineage(_resourceManager.getPropertyStore(), lineage, -1));
  }

  @Test
  public void testDeleteBlockedForInProgressSegmentsFrom() {
    addSegments("ipfA1", "ipfA2", "ipfB1");
    writeLineageEntry("e1", Arrays.asList("ipfA1", "ipfA2"), Collections.singletonList("ipfB1"),
        LineageEntryState.IN_PROGRESS);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Collections.singletonList("ipfA1"));
    assertFalse(response.isSuccessful());
    assertTrue(response.getMessage().contains("ipfA1"));
    // IdealState untouched
    assertTrue(_resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).contains("ipfA1"));
  }

  @Test
  public void testDeleteBlockedForInProgressSegmentsTo() {
    addSegments("iptA1", "iptB1");
    writeLineageEntry("e1", Collections.singletonList("iptA1"), Collections.singletonList("iptB1"),
        LineageEntryState.IN_PROGRESS);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Collections.singletonList("iptB1"));
    assertFalse(response.isSuccessful());
    assertTrue(_resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).contains("iptB1"));
  }

  @Test
  public void testDeleteBlockedForCompletedSegmentsFrom() {
    addSegments("cpfA1", "cpfB1");
    writeLineageEntry("e1", Collections.singletonList("cpfA1"), Collections.singletonList("cpfB1"),
        LineageEntryState.COMPLETED);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Collections.singletonList("cpfA1"));
    assertFalse(response.isSuccessful());
    assertTrue(_resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).contains("cpfA1"));
  }

  @Test
  public void testDeleteAllowedForCompletedSegmentsTo() {
    addSegments("cptA1", "cptB1");
    writeLineageEntry("e1", Collections.singletonList("cptA1"), Collections.singletonList("cptB1"),
        LineageEntryState.COMPLETED);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Collections.singletonList("cptB1"));
    assertTrue(response.isSuccessful(), response.getMessage());
  }

  @Test
  public void testDeleteAllowedForRevertedEntry() {
    addSegments("revA1", "revB1");
    writeLineageEntry("e1", Collections.singletonList("revA1"), Collections.singletonList("revB1"),
        LineageEntryState.REVERTED);
    // Either segmentsFrom or segmentsTo of a REVERTED entry should be unblocked.
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Arrays.asList("revA1", "revB1"));
    assertTrue(response.isSuccessful(), response.getMessage());
  }

  @Test
  public void testBatchRejectionLeavesIdealStateUntouched() {
    addSegments("batchA1", "batchA2", "batchFree");
    writeLineageEntry("e1", Collections.singletonList("batchA1"), Collections.singletonList("batchA2"),
        LineageEntryState.IN_PROGRESS);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Arrays.asList("batchA1", "batchFree"));
    assertFalse(response.isSuccessful());
    // Neither "batchA1" nor "batchFree" should have been removed
    List<String> segments = _resourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false);
    assertTrue(segments.contains("batchA1"));
    assertTrue(segments.contains("batchFree"));
  }

  @Test
  public void testBypassOverloadAllowsBlockedSegments() {
    addSegments("byA1", "byB1");
    writeLineageEntry("e1", Collections.singletonList("byA1"), Collections.singletonList("byB1"),
        LineageEntryState.IN_PROGRESS);
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegmentsForLineageCleanup(OFFLINE_TABLE_NAME, Collections.singletonList("byA1"));
    assertTrue(response.isSuccessful(), response.getMessage());
  }

  @Test
  public void testNoLineageZnodeFastPath() {
    addSegments("nolzA1");
    PinotResourceManagerResponse response =
        _resourceManager.deleteSegments(OFFLINE_TABLE_NAME, Collections.singletonList("nolzA1"));
    assertTrue(response.isSuccessful(), response.getMessage());
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
