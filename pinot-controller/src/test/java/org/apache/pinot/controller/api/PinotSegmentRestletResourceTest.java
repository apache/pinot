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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSegmentRestletResourceTest {
  private static final String TABLE_NAME = "pinotSegmentRestletResourceTestTable";

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testSegmentCrcApi() throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(1).build();
    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);

    // Wait for the table addition
    while (!ControllerTestUtils.getHelixResourceManager().hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    // Check when there is no segment.
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    checkCrcRequest(segmentMetadataTable, 0);

    // Upload Segments
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      ControllerTestUtils.getHelixResourceManager().addNewSegment(TABLE_NAME, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 5);

    // Add more segments
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      ControllerTestUtils.getHelixResourceManager().addNewSegment(TABLE_NAME, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 10);

    // Delete segments
    ControllerTestUtils.getHelixResourceManager().deleteSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
        segmentMetadataTable.values().iterator().next().getName());

    // Check crc api
    checkCrcRequest(segmentMetadataTable, 9);
  }

  private void checkCrcRequest(Map<String, SegmentMetadata> metadataTable, int expectedSize) throws Exception {
    String crcMapStr = ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forListAllCrcInformationForTable(TABLE_NAME));
    Map<String, String> crcMap = JsonUtils.stringToObject(crcMapStr, Map.class);
    for (String segmentName : crcMap.keySet()) {
      SegmentMetadata metadata = metadataTable.get(segmentName);
      Assert.assertTrue(metadata != null);
      Assert.assertEquals(crcMap.get(segmentName), metadata.getCrc());
    }
    Assert.assertEquals(crcMap.size(), expectedSize);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
