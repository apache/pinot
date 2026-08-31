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
package org.apache.pinot.controller.api;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.helix.model.IdealState;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.client.admin.PinotAdminValidationException;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotSegmentRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TEST_RAW_OFFLINE_TABLE_NAME = "offlineTableName1";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testListSegmentLineage()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    // Adding table
    String rawTableName = "lineageTestTable";
    TEST_INSTANCE.addDummySchema(rawTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setNumReplicas(1).build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);

    // Upload Segments
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName, "s" + i);
      resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // There should be no segment lineage at this point.
    String segmentLineageResponse = adminClient.getSegmentClient()
        .listSegmentLineage(rawTableName, TableType.OFFLINE.name());
    assertTrue(segmentLineageResponse.isEmpty() || "{}".equals(segmentLineageResponse)
        || "[]".equals(segmentLineageResponse));

    // Now starts to replace segments.
    List<String> segmentsFrom = Arrays.asList("s0", "s1");
    List<String> segmentsTo = List.of("some_segment");
    String segmentLineageId = resourceManager.startReplaceSegments(offlineTableName, segmentsFrom, segmentsTo, false,
        null);

    // Replace more segments to add another entry to segment lineage.
    segmentsFrom = Arrays.asList("s2", "s3");
    segmentsTo = List.of("another_segment");
    String nextSegmentLineageId =
        resourceManager.startReplaceSegments(offlineTableName, segmentsFrom, segmentsTo, false, null);

    // There should now be two segment lineage entries resulting from the operations above.
    segmentLineageResponse =
        adminClient.getSegmentClient().listSegmentLineage(rawTableName, TableType.OFFLINE.toString());
    assertTrue(segmentLineageResponse.contains("\"state\":\"IN_PROGRESS\""));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s0\",\"s1\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"some_segment\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s2\",\"s3\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"another_segment\"]"));
    SegmentLineage lineage =
        SegmentLineageAccessHelper.getSegmentLineage(resourceManager.getPropertyStore(), offlineTableName);
    // Sort the two entry ids the same way toJsonObject does -- by timestamp, then by id.
    List<String> orderedIds = Stream.of(segmentLineageId, nextSegmentLineageId)
        .sorted(Comparator.comparingLong((String id) -> lineage.getLineageEntry(id).getTimestamp())
            .thenComparing(Comparator.naturalOrder()))
        .toList();
    // Ensures the two entries are sorted in chronological order by timestamp.
    assertTrue(segmentLineageResponse.indexOf(orderedIds.get(0)) < segmentLineageResponse.indexOf(orderedIds.get(1)));

    // List segment lineage should fail for non-existing table
    assertThrows(Exception.class,
        () -> adminClient.getSegmentClient().listSegmentLineage("non-existing-table", TableType.OFFLINE.toString()));

    // List segment lineage should also fail for invalid table type.
    assertThrows(Exception.class,
        () -> adminClient.getSegmentClient().listSegmentLineage(rawTableName, "invalid-type"));
  }

  @Test
  public void testSegmentCrcApi()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    // Adding table
    String rawTableName = "crcTestTable";
    TEST_INSTANCE.addDummySchema(rawTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setNumReplicas(1).build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);

    // Check when there is no segment.
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    checkCrcRequest(adminClient, rawTableName, segmentMetadataTable, 0);

    // Upload Segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName);
      resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(adminClient, rawTableName, segmentMetadataTable, 5);

    // validate the segment metadata
    String sampleSegment = segmentMetadataTable.keySet().iterator().next();
    Map<String, Object> fetchedMetadata =
        adminClient.getSegmentClient().getSegmentMetadata(rawTableName, sampleSegment, null);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // use table name with table type
    fetchedMetadata = adminClient.getSegmentClient().getSegmentMetadata(offlineTableName, sampleSegment, null);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // Add more segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName);
      resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(adminClient, rawTableName, segmentMetadataTable, 10);

    // Delete one segment
    resourceManager.deleteSegment(offlineTableName, sampleSegment);

    // Check crc api
    checkCrcRequest(adminClient, rawTableName, segmentMetadataTable, 9);
  }

  @Test
  public void testDeleteSegmentsWithTimeWindow()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    // Adding table and segment
    String rawTableName = "deleteWithTimeWindowTestTable";
    TEST_INSTANCE.addDummySchema(rawTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setNumReplicas(1)
            .setDeletedSegmentsRetentionPeriod("0d").build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);
    SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName,
        10L, 20L, TimeUnit.MILLISECONDS);
    resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");

    // case 1: no overlapping
    String reply = adminClient.getSegmentClient().deleteSegmentsByTimeWindow(rawTableName, 0L, 10L);
    assertTrue(reply.contains("Deleted 0 segments"));

    // case 2: partial overlapping
    reply = adminClient.getSegmentClient().deleteSegmentsByTimeWindow(rawTableName, 10L, 20L);
    assertTrue(reply.contains("Deleted 0 segments"));

    // case 3: fully within the time window
    reply = adminClient.getSegmentClient().deleteSegmentsByTimeWindow(rawTableName, 10L, 21L);
    assertTrue(reply.contains("Deleted 1 segments"));
  }

  @Test
  public void testDeleteMultipleSegments()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    // Adding table and segment
    TEST_INSTANCE.addDummySchema(TEST_RAW_OFFLINE_TABLE_NAME);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TEST_RAW_OFFLINE_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_RAW_OFFLINE_TABLE_NAME).setNumReplicas(1)
            .setDeletedSegmentsRetentionPeriod("0d").build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);
    SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TEST_RAW_OFFLINE_TABLE_NAME,
        "segment1");
    resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
    segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TEST_RAW_OFFLINE_TABLE_NAME,
        "segment2");
    resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
    segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TEST_RAW_OFFLINE_TABLE_NAME,
        "segment3");
    resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");

    // case 1: send list of segments
    String reply = adminClient.getSegmentClient()
        .deleteMultipleSegments(TEST_RAW_OFFLINE_TABLE_NAME, TableType.OFFLINE.toString(), List.of("segment1"), null);
    assertTrue(reply.contains("Deleted segments: [segment1] from table: offlineTableName1_OFFLINE"));

    // case 2: delete all remaining segments
    reply = adminClient.getSegmentClient()
        .deleteMultipleSegments(TEST_RAW_OFFLINE_TABLE_NAME, TableType.OFFLINE.toString(), List.of(),
            null);
    assertTrue(reply.contains("All segments of table offlineTableName1_OFFLINE deleted"));
  }

  @Test
  public void testRealtimeConsumingSegmentDeleteHttpContracts()
      throws Exception {
    String rawTableName = "consumingSegmentDeleteRest";
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TEST_INSTANCE.addDummySchema(rawTableName);
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    try {
      TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(rawTableName)
          .setDeletedSegmentsRetentionPeriod("0d")
          .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();
      resourceManager.addTable(tableConfig);

      TestUtils.waitForCondition(aVoid -> getConsumingSegment(resourceManager, realtimeTableName) != null, 60_000L,
          "Failed to create a consuming segment for table: " + realtimeTableName);
      String consumingSegment = getConsumingSegment(resourceManager, realtimeTableName);
      assertNotNull(consumingSegment);
      String completedSegment = "completedSegment";
      resourceManager.addNewSegment(realtimeTableName,
          SegmentMetadataMockUtils.mockSegmentMetadata(realtimeTableName, completedSegment), "downloadUrl");
      assertFalse(resourceManager.getTableIdealState(realtimeTableName).getInstanceStateMap(completedSegment)
          .containsValue(SegmentStateModel.CONSUMING));

      PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
      PinotAdminValidationException singleDeleteException = expectThrows(PinotAdminValidationException.class,
          () -> adminClient.getSegmentClient().deleteSegment(realtimeTableName, consumingSegment, null));
      assertTrue(singleDeleteException.getMessage().contains("status: 400"), singleDeleteException.getMessage());
      assertTrue(singleDeleteException.getMessage().contains(consumingSegment), singleDeleteException.getMessage());
      assertTrue(singleDeleteException.getMessage().contains("/tables/{tableName}/pauseStatus"),
          singleDeleteException.getMessage());
      assertTrue(singleDeleteException.getMessage().contains("consumingSegments is empty"),
          singleDeleteException.getMessage());

      URI batchDeleteUri = URI.create(TEST_INSTANCE.getControllerBaseApiUrl() + "/segments/" + rawTableName
          + "?type=REALTIME&segments=" + completedSegment + "&segments=" + consumingSegment);
      SimpleHttpResponse batchDeleteResponse =
          ControllerTest.getHttpClient().sendDeleteRequest(batchDeleteUri, Map.of());
      assertEquals(batchDeleteResponse.getStatusCode(), 400, batchDeleteResponse.getResponse());
      assertTrue(batchDeleteResponse.getResponse().contains(consumingSegment), batchDeleteResponse.getResponse());
      assertTrue(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet()
          .containsAll(Set.of(completedSegment, consumingSegment)));

      LLCSegmentName currentConsumingSegment = new LLCSegmentName(consumingSegment);
      String onlineLowerPartitionSegment = new LLCSegmentName(rawTableName,
          currentConsumingSegment.getPartitionGroupId(), currentConsumingSegment.getSequenceNumber() + 1,
          System.currentTimeMillis()).getSegmentName();
      String consumingHigherPartitionSegment = new LLCSegmentName(rawTableName,
          currentConsumingSegment.getPartitionGroupId() + 1, 0, System.currentTimeMillis()).getSegmentName();
      HelixHelper.updateIdealState(TEST_INSTANCE.getHelixManager(), realtimeTableName, idealState -> {
        String server = idealState.getInstanceStateMap(consumingSegment).keySet().iterator().next();
        idealState.getRecord().setMapField(onlineLowerPartitionSegment,
            new HashMap<>(Map.of(server, SegmentStateModel.ONLINE)));
        idealState.getRecord().setMapField(consumingHigherPartitionSegment,
            new HashMap<>(Map.of(server, SegmentStateModel.CONSUMING)));
        return idealState;
      });

      URI forceDeleteUri = URI.create(TEST_INSTANCE.getControllerBaseApiUrl()
          + "/deleteSegmentsFromSequenceNum/" + realtimeTableName + "?segments=" + onlineLowerPartitionSegment
          + "&segments=" + consumingHigherPartitionSegment + "&dryRun=false&force=true");
      SimpleHttpResponse forceDeleteResponse =
          ControllerTest.getHttpClient().sendDeleteRequest(forceDeleteUri, Map.of());
      assertEquals(forceDeleteResponse.getStatusCode(), 400, forceDeleteResponse.getResponse());
      assertTrue(forceDeleteResponse.getResponse().contains(consumingHigherPartitionSegment),
          forceDeleteResponse.getResponse());
      assertTrue(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet()
          .containsAll(Set.of(onlineLowerPartitionSegment, consumingHigherPartitionSegment)));

      Set<String> beforeDeleteAll =
          Set.copyOf(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet());
      PinotAdminValidationException deleteAllException = expectThrows(PinotAdminValidationException.class,
          () -> adminClient.getSegmentClient().deleteMultipleSegments(rawTableName, TableType.REALTIME.name(),
              List.of(), null));
      assertTrue(deleteAllException.getMessage().contains("status: 400"), deleteAllException.getMessage());
      assertEquals(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet(), beforeDeleteAll);

      String successResponse =
          adminClient.getSegmentClient().deleteSegment(realtimeTableName, completedSegment, null);
      assertTrue(successResponse.contains("Segment deleted"), successResponse);
      assertFalse(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet().contains(completedSegment));
      assertTrue(resourceManager.getTableIdealState(realtimeTableName).getPartitionSet().contains(consumingSegment));

      String failingSegment = "failingCompletedSegment";
      resourceManager.addNewSegment(realtimeTableName,
          SegmentMetadataMockUtils.mockSegmentMetadata(realtimeTableName, failingSegment), "downloadUrl");
      SegmentDeletionManager failingDeletionManager = mock(SegmentDeletionManager.class);
      doThrow(new RuntimeException("test deletion failure")).when(failingDeletionManager)
          .deleteSegments(eq(realtimeTableName), anyCollection(), nullable(Long.class));
      SegmentDeletionManager originalDeletionManager = replaceSegmentDeletionManager(resourceManager,
          failingDeletionManager);
      try {
        PinotAdminException internalError = expectThrows(PinotAdminException.class,
            () -> adminClient.getSegmentClient().deleteSegment(realtimeTableName, failingSegment, "0d"));
        assertTrue(internalError.getMessage().contains("status: 500"), internalError.getMessage());
      } finally {
        replaceSegmentDeletionManager(resourceManager, originalDeletionManager);
      }

      // Dropping a realtime table remains the supported unconditional cleanup path.
      resourceManager.deleteRealtimeTable(rawTableName, "0d");
      assertNull(resourceManager.getTableIdealState(realtimeTableName));
    } finally {
      try {
        if (resourceManager.getTableIdealState(realtimeTableName) != null) {
          resourceManager.deleteRealtimeTable(rawTableName, "0d");
        }
      } finally {
        TEST_INSTANCE.deleteSchema(rawTableName);
      }
    }
  }

  private static String getConsumingSegment(PinotHelixResourceManager resourceManager, String realtimeTableName) {
    IdealState idealState = resourceManager.getTableIdealState(realtimeTableName);
    if (idealState == null) {
      return null;
    }
    for (String segmentName : idealState.getPartitionSet()) {
      Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
      if (stateMap != null && stateMap.containsValue(SegmentStateModel.CONSUMING)) {
        return segmentName;
      }
    }
    return null;
  }

  private static SegmentDeletionManager replaceSegmentDeletionManager(PinotHelixResourceManager resourceManager,
      SegmentDeletionManager replacement)
      throws ReflectiveOperationException {
    Field field = PinotHelixResourceManager.class.getDeclaredField("_segmentDeletionManager");
    field.setAccessible(true);
    SegmentDeletionManager previous = (SegmentDeletionManager) field.get(resourceManager);
    field.set(resourceManager, replacement);
    return previous;
  }

  private void checkCrcRequest(PinotAdminClient adminClient, String tableName,
      Map<String, SegmentMetadata> metadataTable, int expectedSize)
      throws Exception {
    Map<String, String> crcMap = adminClient.getSegmentClient().getSegmentToCrcMap(tableName);
    if (crcMap == null) {
      crcMap = java.util.Map.of();
    }
    for (String segmentName : crcMap.keySet()) {
      SegmentMetadata metadata = metadataTable.get(segmentName);
      assertNotNull(metadata);
      assertEquals(crcMap.get(segmentName), metadata.getCrc());
    }
    assertEquals(crcMap.size(), expectedSize);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
