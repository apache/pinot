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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PinotSegmentRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testListSegmentLineage()
      throws Exception {
    // Adding table
    String rawTableName = "lineageTestTable";
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
    ControllerRequestURLBuilder urlBuilder = TEST_INSTANCE.getControllerRequestURLBuilder();
    String segmentLineageResponse =
        ControllerTest.sendGetRequest(urlBuilder.forListAllSegmentLineages(rawTableName, TableType.OFFLINE.name()));
    assertEquals(segmentLineageResponse, "");

    // Now starts to replace segments.
    List<String> segmentsFrom = Arrays.asList("s0", "s1");
    List<String> segmentsTo = Collections.singletonList("some_segment");
    String segmentLineageId = resourceManager.startReplaceSegments(offlineTableName, segmentsFrom, segmentsTo, false,
        null);

    // Replace more segments to add another entry to segment lineage.
    segmentsFrom = Arrays.asList("s2", "s3");
    segmentsTo = Collections.singletonList("another_segment");
    String nextSegmentLineageId =
        resourceManager.startReplaceSegments(offlineTableName, segmentsFrom, segmentsTo, false, null);

    // There should now be two segment lineage entries resulting from the operations above.
    segmentLineageResponse =
        ControllerTest.sendGetRequest(urlBuilder.forListAllSegmentLineages(rawTableName, TableType.OFFLINE.toString()));
    assertTrue(segmentLineageResponse.contains("\"state\":\"IN_PROGRESS\""));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s0\",\"s1\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"some_segment\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s2\",\"s3\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"another_segment\"]"));
    // Ensures the two entries are sorted in chronological order by timestamp.
    assertTrue(segmentLineageResponse.indexOf(segmentLineageId) < segmentLineageResponse.indexOf(nextSegmentLineageId));

    // List segment lineage should fail for non-existing table
    assertThrows(IOException.class, () -> ControllerTest.sendGetRequest(
        urlBuilder.forListAllSegmentLineages("non-existing-table", TableType.OFFLINE.toString())));

    // List segment lineage should also fail for invalid table type.
    assertThrows(IOException.class,
        () -> ControllerTest.sendGetRequest(urlBuilder.forListAllSegmentLineages(rawTableName, "invalid-type")));
  }

  @Test
  public void testSegmentCrcApi()
      throws Exception {
    // Adding table
    String rawTableName = "crcTestTable";
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setNumReplicas(1).build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);

    // Check when there is no segment.
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    checkCrcRequest(rawTableName, segmentMetadataTable, 0);

    // Upload Segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName);
      resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(rawTableName, segmentMetadataTable, 5);

    // validate the segment metadata
    String sampleSegment = segmentMetadataTable.keySet().iterator().next();
    String resp = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSegmentMetadata(rawTableName, sampleSegment));
    Map<String, String> fetchedMetadata = JsonUtils.stringToObject(resp, Map.class);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // use table name with table type
    resp = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSegmentMetadata(offlineTableName, sampleSegment));
    fetchedMetadata = JsonUtils.stringToObject(resp, Map.class);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // Add more segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName);
      resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(rawTableName, segmentMetadataTable, 10);

    // Delete one segment
    resourceManager.deleteSegment(offlineTableName, sampleSegment);

    // Check crc api
    checkCrcRequest(rawTableName, segmentMetadataTable, 9);
  }

  @Test
  public void testDeleteSegmentsWithTimeWindow()
      throws Exception {
    // Adding table and segment
    String rawTableName = "deleteWithTimeWindowTestTable";
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setNumReplicas(1)
            .setDeletedSegmentsRetentionPeriod("0d").build();
    PinotHelixResourceManager resourceManager = TEST_INSTANCE.getHelixResourceManager();
    resourceManager.addTable(tableConfig);
    SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName,
        10L, 20L, TimeUnit.MILLISECONDS);
    resourceManager.addNewSegment(offlineTableName, segmentMetadata, "downloadUrl");

    // Send query and verify
    ControllerRequestURLBuilder urlBuilder = TEST_INSTANCE.getControllerRequestURLBuilder();
    // case 1: no overlapping
    String reply = ControllerTest.sendDeleteRequest(urlBuilder.forSegmentDeleteWithTimeWindowAPI(
        rawTableName, 0L, 10L));
    assertTrue(reply.contains("Deleted 0 segments"));

    // case 2: partial overlapping
    reply = ControllerTest.sendDeleteRequest(urlBuilder.forSegmentDeleteWithTimeWindowAPI(
        rawTableName, 10L, 20L));
    assertTrue(reply.contains("Deleted 0 segments"));

    // case 3: fully within the time window
    reply = ControllerTest.sendDeleteRequest(urlBuilder.forSegmentDeleteWithTimeWindowAPI(
        rawTableName, 10L, 21L));
    assertTrue(reply.contains("Deleted 1 segments"));
  }

  private void checkCrcRequest(String tableName, Map<String, SegmentMetadata> metadataTable, int expectedSize)
      throws Exception {
    String crcMapStr = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forListAllCrcInformationForTable(tableName));
    Map<String, String> crcMap = JsonUtils.stringToObject(crcMapStr, Map.class);
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
