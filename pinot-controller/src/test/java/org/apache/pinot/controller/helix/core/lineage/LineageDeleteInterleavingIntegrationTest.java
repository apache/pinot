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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.controller.util.BrokerServiceHelper;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Integration tests for the lineage-aware segment delete check at the REST layer and across an interleaved
 * start/end/revert lifecycle.
 *
 * <p>This test:
 * <ul>
 *   <li>Drives every step through the REST endpoints.</li>
 *   <li>Walks a single table through all three lineage states with a delete attempt at every step.</li>
 *   <li>Re-reads the lineage znode after every API call (success or failure) to verify failed requests never
 *       mutate the entry.</li>
 *   <li>Verifies the retention path silently skips lineage-locked segments and that the lineage-cleanup pass
 *       uses the bypass delete path.</li>
 * </ul>
 *
 * <p>Each test uses unique segment names so the asynchronous {@code SegmentDeletionManager} cleanup cannot
 * interfere with later tests.
 */
public class LineageDeleteInterleavingIntegrationTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String RAW_TABLE_NAME = "lineageInterleavingTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  // Separate table with retention configured, used by the retention-interaction test so it does not affect the
  // other scenarios that should never trigger time-based deletion.
  private static final String RETENTION_RAW_TABLE_NAME = "lineageInterleavingRetentionTable";
  private static final String RETENTION_OFFLINE_TABLE_NAME =
      TableNameBuilder.OFFLINE.tableNameWithType(RETENTION_RAW_TABLE_NAME);

  private PinotHelixResourceManager _resourceManager;
  private String _controllerBaseUrl;
  private TestableRetentionManager _retentionManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    _resourceManager = TEST_INSTANCE.getHelixResourceManager();
    _controllerBaseUrl = TEST_INSTANCE.getControllerBaseApiUrl();

    TEST_INSTANCE.addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(1).build();
    _resourceManager.addTable(tableConfig);

    TEST_INSTANCE.addDummySchema(RETENTION_RAW_TABLE_NAME);
    // replacedSegmentsRetentionPeriod is set to 0s so the lineage-cleanup pass deletes replaced segments as soon
    // as the entry has a timestamp strictly older than "now". This keeps the test deterministic without having to
    // sleep through the default retention window (4 hours for this APPEND table).
    TableConfig retentionTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RETENTION_RAW_TABLE_NAME).setNumReplicas(1)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setReplacedSegmentsRetentionPeriod("0s").build();
    _resourceManager.addTable(retentionTableConfig);

    // RetentionManager configured with zero frequencies so we can drive processTable() directly and validate the
    // lineage interaction in isolation.
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setRetentionControllerFrequencyInSeconds(0);
    controllerConf.setDeletedSegmentsRetentionInDays(0);
    BrokerServiceHelper brokerServiceHelper = new BrokerServiceHelper(_resourceManager, controllerConf, null, null);
    _retentionManager = new TestableRetentionManager(_resourceManager, mock(LeadControllerManager.class),
        controllerConf, mock(ControllerMetrics.class), brokerServiceHelper);
  }

  /**
   * Subclass that exposes the package-protected {@link RetentionManager#processTable(String)} so this test
   * (in a different package) can drive a single retention pass against a single table.
   */
  private static final class TestableRetentionManager extends RetentionManager {
    TestableRetentionManager(PinotHelixResourceManager pinotHelixResourceManager,
        LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
        BrokerServiceHelper brokerServiceHelper) {
      super(pinotHelixResourceManager, leadControllerManager, config, controllerMetrics, brokerServiceHelper);
    }

    void runProcessTable(String tableNameWithType) {
      processTable(tableNameWithType);
    }
  }

  @BeforeMethod
  public void clearLineage() {
    SegmentLineageAccessHelper.deleteSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    SegmentLineageAccessHelper.deleteSegmentLineage(_resourceManager.getPropertyStore(),
        RETENTION_OFFLINE_TABLE_NAME);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T1: full lifecycle interleaved with delete attempts at every step
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testFullLifecycleInterleavedWithDeleteAttempts()
      throws IOException {
    String s1 = "t1_s1";
    String s2 = "t1_s2";
    String s3 = "t1_s3";
    String s1New = "t1_s1_new";

    addSegments(OFFLINE_TABLE_NAME, s1, s2, s3);

    // Step 1: startReplaceSegments → IN_PROGRESS
    String entryId = postStartReplaceSegments(OFFLINE_TABLE_NAME, List.of(s1),
        List.of(s1New));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s1New));

    // Step 2: delete s1 (segmentsFrom of IN_PROGRESS) → 500, IdealState + lineage untouched
    IOException ex500 = expectThrows(IOException.class, () -> sendDeleteSegment(OFFLINE_TABLE_NAME, s1));
    assertStatus(ex500, 500);
    assertTrue(getSegments(OFFLINE_TABLE_NAME).contains(s1));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s1New));

    // Step 3: upload s1New + delete s1New (segmentsTo of IN_PROGRESS) → 500.
    // the lineage check still rejects without touching the lineage znode.
    addSegments(OFFLINE_TABLE_NAME, s1New);
    ex500 = expectThrows(IOException.class, () -> sendDeleteSegment(OFFLINE_TABLE_NAME, s1New));
    assertStatus(ex500, 500);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s1New));

    // Step 4: endReplaceSegments → COMPLETED
    postEndReplaceSegments(OFFLINE_TABLE_NAME, entryId);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.COMPLETED, List.of(s1),
        List.of(s1New));

    // Step 5: delete s1 (segmentsFrom of COMPLETED) → still 500
    ex500 = expectThrows(IOException.class, () -> sendDeleteSegment(OFFLINE_TABLE_NAME, s1));
    assertStatus(ex500, 500);
    assertTrue(getSegments(OFFLINE_TABLE_NAME).contains(s1));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.COMPLETED, List.of(s1),
        List.of(s1New));

    // Step 6: delete s1New (segmentsTo of COMPLETED) → 200 (segmentsTo is NOT locked once the entry is COMPLETED).
    // The lineage entry itself must NOT mutate as a side effect of the delete.
    sendDeleteSegment(OFFLINE_TABLE_NAME, s1New);
    assertFalse(getSegments(OFFLINE_TABLE_NAME).contains(s1New));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.COMPLETED, List.of(s1),
        List.of(s1New));

    // Step 7: revertReplaceSegments (with forceRevert so it can revert a COMPLETED entry safely) → REVERTED
    postRevertReplaceSegments(OFFLINE_TABLE_NAME, entryId, true);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.REVERTED, List.of(s1),
        List.of(s1New));

    // Step 8: delete s1 (REVERTED locks nothing) → 200
    sendDeleteSegment(OFFLINE_TABLE_NAME, s1);
    assertFalse(getSegments(OFFLINE_TABLE_NAME).contains(s1));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.REVERTED, List.of(s1),
        List.of(s1New));

    // Final: IdealState contains exactly s2, s3; lineage has exactly one entry in REVERTED state
    List<String> finalSegments = getSegments(OFFLINE_TABLE_NAME);
    assertTrue(finalSegments.contains(s2));
    assertTrue(finalSegments.contains(s3));
    SegmentLineage lineage =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), OFFLINE_TABLE_NAME);
    assertEquals(lineage.getLineageEntryIds().size(), 1);
    assertEquals(lineage.getLineageEntry(entryId).getState(), LineageEntryState.REVERTED);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T2: REST-level batch delete rejection atomicity
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testBatchDeleteRejectionAtomicityOverRest()
      throws IOException {
    String s1 = "t2_s1";
    String s1New = "t2_s1_new";
    String sFree = "t2_sFree";

    addSegments(OFFLINE_TABLE_NAME, s1, sFree);
    String entryId = postStartReplaceSegments(OFFLINE_TABLE_NAME, List.of(s1),
        List.of(s1New));

    // Batch delete via DELETE /segments/{tableName}?type=OFFLINE&segments=s1&segments=sFree → 500
    String url = _controllerBaseUrl + "/segments/" + RAW_TABLE_NAME + "?type=OFFLINE&segments=" + s1
        + "&segments=" + sFree;
    IOException ex500 = expectThrows(IOException.class, () -> ControllerTest.sendDeleteRequest(url));
    assertStatus(ex500, 500);
    // Body should mention the blocking segment, but not the free one (the free one is not what's blocking).
    String body = ex500.getMessage();
    assertTrue(body.contains(s1), "Expected response to mention blocking segment: " + s1 + ", was: " + body);
    assertFalse(body.contains("Blocking segments: [" + sFree),
        "Free segment should not appear in blocking list. Was: " + body);

    // Neither s1 nor sFree was removed
    List<String> segments = getSegments(OFFLINE_TABLE_NAME);
    assertTrue(segments.contains(s1));
    assertTrue(segments.contains(sFree));

    // Lineage entry is untouched
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s1New));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T3: Recovery via forceRevert when a (bypass) concurrent delete strands the lineage entry
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testRecoveryViaForceRevertAfterCorruptedLineage()
      throws IOException {
    String s1 = "t4_s1";
    String s2 = "t4_s2";
    addSegments(OFFLINE_TABLE_NAME, s1);

    String entryId = postStartReplaceSegments(OFFLINE_TABLE_NAME, List.of(s1),
        List.of(s2));
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s2));

    // Simulate the lost-race outcome via the bypass overload (the only path that can produce this state — the
    // public REST DELETE would be rejected by the new check).
    _resourceManager.deleteSegmentsForLineageCleanup(OFFLINE_TABLE_NAME, List.of(s1));
    assertFalse(getSegments(OFFLINE_TABLE_NAME).contains(s1));
    // Lineage entry must NOT mutate as a side effect of the bypass delete.
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s2));

    // Upload s2; lineage still unchanged
    addSegments(OFFLINE_TABLE_NAME, s2);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s2));

    // endReplaceSegments must fail because s1 (segmentsFrom) is gone.
    IOException endEx = expectThrows(IOException.class, () -> postEndReplaceSegments(OFFLINE_TABLE_NAME, entryId));
    assertNon2xx(endEx);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s2));

    // Operator recovery via revertReplaceSegments(forceRevert=true) → REVERTED
    postRevertReplaceSegments(OFFLINE_TABLE_NAME, entryId, true);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.REVERTED, List.of(s1),
        List.of(s2));

    // s2 is now unblocked — delete via the public path
    sendDeleteSegment(OFFLINE_TABLE_NAME, s2);
    assertFalse(getSegments(OFFLINE_TABLE_NAME).contains(s2));
    // Delete must not touch the lineage entry
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.REVERTED, List.of(s1),
        List.of(s2));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T4b: revertReplaceSegments without forceRevert must NOT revert an IN_PROGRESS entry, and a failed revert must
  // not corrupt the znode either
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testRevertWithoutForceRevertFailsOnCorruptedLineage()
      throws IOException {
    String s1 = "t4b_s1";
    String s2 = "t4b_s2";
    addSegments(OFFLINE_TABLE_NAME, s1);

    String entryId = postStartReplaceSegments(OFFLINE_TABLE_NAME, List.of(s1),
        List.of(s2));
    _resourceManager.deleteSegmentsForLineageCleanup(OFFLINE_TABLE_NAME, List.of(s1));
    addSegments(OFFLINE_TABLE_NAME, s2);

    // Attempting revertReplaceSegments without forceRevert against an IN_PROGRESS entry should fail.
    IOException ex = expectThrows(IOException.class, () -> postRevertReplaceSegments(OFFLINE_TABLE_NAME, entryId,
        false));
    assertNon2xx(ex);
    // Failed revert must not mutate the znode.
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
        List.of(s2));

    // Retry with forceRevert=true → 200
    postRevertReplaceSegments(OFFLINE_TABLE_NAME, entryId, true);
    assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.REVERTED, List.of(s1),
        List.of(s2));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T5: retention manager skips lineage-locked segments and the lineage-cleanup pass uses the bypass path
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testRetentionSkipsLineageLockedSegmentsAndCleansUpViaBypass() {
    String sExpired = "t5_expired";
    String sExpiredFree = "t5_expired_free";
    String sKept = "t5_kept";
    String sNew = "t5_new";

    // sExpired:     end time 30 days ago → retention-eligible AND lineage-locked
    // sExpiredFree: end time 30 days ago → retention-eligible, NOT lineage-locked
    // sKept:        end time = now       → not retention-eligible
    long nowMs = System.currentTimeMillis();
    long thirtyDaysAgoMs = nowMs - TimeUnit.DAYS.toMillis(30L);
    addSegmentWithTime(RETENTION_OFFLINE_TABLE_NAME, sExpired, thirtyDaysAgoMs - TimeUnit.DAYS.toMillis(1L),
        thirtyDaysAgoMs);
    addSegmentWithTime(RETENTION_OFFLINE_TABLE_NAME, sExpiredFree, thirtyDaysAgoMs - TimeUnit.DAYS.toMillis(1L),
        thirtyDaysAgoMs);
    addSegmentWithTime(RETENTION_OFFLINE_TABLE_NAME, sKept, nowMs - TimeUnit.HOURS.toMillis(1L), nowMs);

    // Lock sExpired in an IN_PROGRESS lineage entry → segmentsFrom is lineage-locked
    String entryId = _resourceManager.startReplaceSegments(RETENTION_OFFLINE_TABLE_NAME,
        List.of(sExpired), List.of(sNew), false, null);
    assertNotNull(entryId);

    // Run retention. Time-based purge picks up both expired segments, but removeLineageLockedSegments strips
    // sExpired out before the delete call — the rest of the batch (sExpiredFree) must still be deleted, proving
    // the lineage-lock filtering does not poison the whole retention pass. The lineage-cleanup pass also does
    // nothing yet because the entry is IN_PROGRESS and not aged enough.
    _retentionManager.runProcessTable(RETENTION_OFFLINE_TABLE_NAME);
    List<String> segmentsAfterFirstPass = getSegments(RETENTION_OFFLINE_TABLE_NAME);
    assertTrue(segmentsAfterFirstPass.contains(sExpired),
        "Retention must skip sExpired while it participates in a live lineage entry");
    assertFalse(segmentsAfterFirstPass.contains(sExpiredFree),
        "Retention must still delete the non-lineage-locked expired segment in the same pass");
    assertTrue(segmentsAfterFirstPass.contains(sKept));
    assertLineageEntry(RETENTION_OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS,
        List.of(sExpired), List.of(sNew));

    // Complete the replacement. sNew has a fresh end-time so it is not retention-eligible.
    addSegmentWithTime(RETENTION_OFFLINE_TABLE_NAME, sNew, nowMs - TimeUnit.HOURS.toMillis(1L), nowMs);
    _resourceManager.endReplaceSegments(RETENTION_OFFLINE_TABLE_NAME, entryId, null);
    assertLineageEntry(RETENTION_OFFLINE_TABLE_NAME, entryId, LineageEntryState.COMPLETED,
        List.of(sExpired), List.of(sNew));
    // endReplaceSegments does NOT proactively clean up segmentsFrom; that's the retention path's responsibility.
    assertTrue(getSegments(RETENTION_OFFLINE_TABLE_NAME).contains(sExpired));

    // Run retention again. Now the COMPLETED entry's segmentsFrom is still lineage-locked w.r.t. time-based
    // purge, but the lineage-cleanup pass is allowed to delete it (the table's replacedSegmentsRetentionPeriod
    // is configured to 0s, so the COMPLETED entry exits its retention window as soon as any time has elapsed).
    // The cleanup must go through deleteSegmentsForLineageCleanup so it isn't self-blocked by the new check.
    _retentionManager.runProcessTable(RETENTION_OFFLINE_TABLE_NAME);
    // IdealState updates inside deleteSegmentsForLineageCleanup are synchronous (only the deep-store file deletion
    // is async via SegmentDeletionManager), so we can assert directly.
    assertFalse(getSegments(RETENTION_OFFLINE_TABLE_NAME).contains(sExpired),
        "Expected sExpired to be removed from IdealState by the lineage-cleanup pass");
    assertTrue(getSegments(RETENTION_OFFLINE_TABLE_NAME).contains(sNew));
    assertTrue(segmentsAfterFirstPass.contains(sKept));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // T6: Kill switch — deleting a lineage-locked segment is allowed when
  //     controller.lineage.exclusive.delete.enabled=false
  // ---------------------------------------------------------------------------------------------------------------
  @Test
  public void testDeleteAllowedWhenLineageExclusiveDeleteDisabled()
      throws IOException {
    String s1 = "t6_s1";
    String s1New = "t6_s1_new";
    addSegments(OFFLINE_TABLE_NAME, s1);
    String entryId = postStartReplaceSegments(OFFLINE_TABLE_NAME, List.of(s1),
        List.of(s1New));

    // PinotHelixResourceManager.isLineageExclusiveDeleteEnabled() reads from the live ControllerConf on every
    // delete (no caching), so toggling the property here flips the behavior for the next REST call. Reset in a
    // finally block so other tests on this shared singleton are not affected by the override.
    ControllerConf conf = TEST_INSTANCE.getControllerConfig();
    conf.setProperty(ControllerConf.LINEAGE_EXCLUSIVE_DELETE_ENABLED, false);
    try {
      sendDeleteSegment(OFFLINE_TABLE_NAME, s1);
      assertFalse(getSegments(OFFLINE_TABLE_NAME).contains(s1),
          "Segment must be removed from IdealState when lineage-exclusive-delete is disabled");
      // The lineage entry must not be mutated as a side effect of the delete.
      assertLineageEntry(OFFLINE_TABLE_NAME, entryId, LineageEntryState.IN_PROGRESS, List.of(s1),
          List.of(s1New));
    } finally {
      conf.setProperty(ControllerConf.LINEAGE_EXCLUSIVE_DELETE_ENABLED, true);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------------------------

  private void addSegments(String tableNameWithType, String... segmentNames) {
    for (String name : segmentNames) {
      _resourceManager.addNewSegment(tableNameWithType,
          SegmentMetadataMockUtils.mockSegmentMetadata(tableNameWithType, name), "downloadUrl");
    }
  }

  private void addSegmentWithTime(String tableNameWithType, String segmentName, long startTimeMs, long endTimeMs) {
    String crc = Long.toString(System.nanoTime());
    SegmentMetadata metadata = SegmentMetadataMockUtils.mockSegmentMetadata(tableNameWithType, segmentName, 100, crc,
        startTimeMs, endTimeMs, TimeUnit.MILLISECONDS);
    _resourceManager.addNewSegment(tableNameWithType, metadata, "downloadUrl");
  }

  private List<String> getSegments(String tableNameWithType) {
    return _resourceManager.getSegmentsFor(tableNameWithType, false);
  }

  private void assertLineageEntry(String tableNameWithType, String entryId, LineageEntryState expectedState,
      List<String> expectedSegmentsFrom, List<String> expectedSegmentsTo) {
    SegmentLineage lineage =
        SegmentLineageAccessHelper.getSegmentLineage(_resourceManager.getPropertyStore(), tableNameWithType);
    assertNotNull(lineage, "Expected lineage znode to exist for table: " + tableNameWithType);
    LineageEntry entry = lineage.getLineageEntry(entryId);
    assertNotNull(entry, "Expected lineage entry: " + entryId + " for table: " + tableNameWithType);
    assertEquals(entry.getState(), expectedState,
        "Unexpected state for lineage entry: " + entryId + " on table: " + tableNameWithType);
    assertEquals(entry.getSegmentsFrom(), expectedSegmentsFrom,
        "Unexpected segmentsFrom for lineage entry: " + entryId);
    assertEquals(entry.getSegmentsTo(), expectedSegmentsTo,
        "Unexpected segmentsTo for lineage entry: " + entryId);
  }

  private String postStartReplaceSegments(String tableNameWithType, List<String> segmentsFrom,
      List<String> segmentsTo)
      throws IOException {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String url = _controllerBaseUrl + "/segments/" + rawTableName + "/startReplaceSegments?type=OFFLINE";
    String body = JsonUtils.objectToString(new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo));
    String response = ControllerTest.sendPostRequest(url, body);
    JsonNode json = JsonUtils.stringToJsonNode(response);
    JsonNode entryIdNode = json.get("segmentLineageEntryId");
    assertNotNull(entryIdNode, "Expected segmentLineageEntryId in response. Was: " + response);
    return entryIdNode.asText();
  }

  private void postEndReplaceSegments(String tableNameWithType, String entryId)
      throws IOException {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String url = _controllerBaseUrl + "/segments/" + rawTableName
        + "/endReplaceSegments?type=OFFLINE&segmentLineageEntryId=" + entryId;
    ControllerTest.sendPostRequest(url, "");
  }

  private void postRevertReplaceSegments(String tableNameWithType, String entryId, boolean forceRevert)
      throws IOException {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String url = _controllerBaseUrl + "/segments/" + rawTableName
        + "/revertReplaceSegments?type=OFFLINE&segmentLineageEntryId=" + entryId + "&forceRevert=" + forceRevert;
    ControllerTest.sendPostRequest(url, "");
  }

  private void sendDeleteSegment(String tableNameWithType, String segmentName)
      throws IOException {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String url = _controllerBaseUrl + "/segments/" + rawTableName + "/" + segmentName;
    ControllerTest.sendDeleteRequest(url);
  }

  private void assertStatus(IOException e, int expectedStatus) {
    String msg = e.getMessage() != null ? e.getMessage() : "";
    assertTrue(msg.contains("status code: " + expectedStatus) || msg.contains("status: " + expectedStatus),
        "Expected status " + expectedStatus + " but got: " + msg);
  }

  private void assertNon2xx(IOException e) {
    String msg = e.getMessage() != null ? e.getMessage() : "";
    // The HttpClient error message starts with "Got error status code: <N>" for any non-2xx response.
    assertTrue(msg.contains("Got error status code"), "Expected a non-2xx HTTP error but got: " + msg);
    assertFalse(Arrays.asList("200", "201", "202", "204").stream().anyMatch(
        code -> msg.contains("status code: " + code + " ")), "Expected non-2xx response but got: " + msg);
  }
}
