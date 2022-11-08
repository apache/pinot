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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PinotSegmentRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME = "pinotSegmentRestletResourceTestTable";
  private static final String TABLE_NAME_OFFLINE = TABLE_NAME + "_OFFLINE";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testListSegmentLineage()
      throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(1).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    // Wait for the table addition
    while (!TEST_INSTANCE.getHelixResourceManager().hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();

    // Upload Segments
    for (int i = 0; i < 4; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME, "s" + i);
      TEST_INSTANCE.getHelixResourceManager()
          .addNewSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // There should be no segment lineage at this point.
    String segmentLineageResponse = ControllerTest.sendGetRequest(TEST_INSTANCE.getControllerRequestURLBuilder()
        .forListAllSegmentLineages(TABLE_NAME, TableType.OFFLINE.toString()));
    assertEquals(segmentLineageResponse, "");

    // Now starts to replace segments.
    List<String> segmentsFrom = Arrays.asList("s0", "s1");
    List<String> segmentsTo = Arrays.asList("some_segment");
    String segmentLineageId = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(TABLE_NAME_OFFLINE, segmentsFrom, segmentsTo, false);

    // Replace more segments to add another entry to segment lineage.
    segmentsFrom = Arrays.asList("s2", "s3");
    segmentsTo = Arrays.asList("another_segment");
    String nextSegmentLineageId = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(TABLE_NAME_OFFLINE, segmentsFrom, segmentsTo, false);

    // There should now be two segment lineage entries resulting from the operations above.
    segmentLineageResponse = ControllerTest.sendGetRequest(TEST_INSTANCE.getControllerRequestURLBuilder()
        .forListAllSegmentLineages(TABLE_NAME, TableType.OFFLINE.toString()));
    assertTrue(segmentLineageResponse.contains("\"state\":\"IN_PROGRESS\""));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s0\",\"s1\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"some_segment\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"s2\",\"s3\"]"));
    assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"another_segment\"]"));
    // Ensures the two entries are sorted in chronological order by timestamp.
    assertTrue(segmentLineageResponse.indexOf(segmentLineageId) < segmentLineageResponse.indexOf(nextSegmentLineageId));

    // List segment lineage should fail for non-existing table
    assertThrows(IOException.class, () -> ControllerTest.sendGetRequest(TEST_INSTANCE.getControllerRequestURLBuilder()
        .forListAllSegmentLineages("non-existing-table", TableType.OFFLINE.toString())));

    // List segment lineage should also fail for invalid table type.
    assertThrows(IOException.class, () -> ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forListAllSegmentLineages(TABLE_NAME, "invalid-type")));

    // Delete segments
    TEST_INSTANCE.getHelixResourceManager().deleteSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
        segmentMetadataTable.values().iterator().next().getName());

    // Delete offline table
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);
  }

  @Test
  public void testSegmentCrcApi()
      throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(1).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    // Wait for the table addition
    while (!TEST_INSTANCE.getHelixResourceManager().hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    // Check when there is no segment.
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    checkCrcRequest(segmentMetadataTable, 0);

    // Upload Segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      TEST_INSTANCE.getHelixResourceManager()
          .addNewSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 5);

    // validate the segment metadata
    Map.Entry<String, SegmentMetadata> entry =
        (Map.Entry<String, SegmentMetadata>) segmentMetadataTable.entrySet().toArray()[0];
    String resp = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSegmentMetadata(TABLE_NAME, entry.getKey()));
    Map<String, String> fetchedMetadata = JsonUtils.stringToObject(resp, Map.class);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // use table name with table type
    resp = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSegmentMetadata(TABLE_NAME + "_OFFLINE", entry.getKey()));
    fetchedMetadata = JsonUtils.stringToObject(resp, Map.class);
    assertEquals(fetchedMetadata.get("segment.download.url"), "downloadUrl");

    // Add more segments
    for (int i = 0; i < 5; i++) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      TEST_INSTANCE.getHelixResourceManager()
          .addNewSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 10);

    // Delete segments
    TEST_INSTANCE.getHelixResourceManager().deleteSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
        segmentMetadataTable.values().iterator().next().getName());

    // Check crc api
    checkCrcRequest(segmentMetadataTable, 9);

    // Delete offline table
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);
  }

  private void checkCrcRequest(Map<String, SegmentMetadata> metadataTable, int expectedSize)
      throws Exception {
    String crcMapStr = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forListAllCrcInformationForTable(TABLE_NAME));
    Map<String, String> crcMap = JsonUtils.stringToObject(crcMapStr, Map.class);
    for (String segmentName : crcMap.keySet()) {
      SegmentMetadata metadata = metadataTable.get(segmentName);
      assertTrue(metadata != null);
      assertEquals(crcMap.get(segmentName), metadata.getCrc());
    }
    assertEquals(crcMap.size(), expectedSize);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
