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
package org.apache.pinot.controller.api.resources;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSegmentRestletResourceTest extends ControllerTest {
  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String TABLE_NAME = "testTable";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();

    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_SERVER, 1, true);
    ControllerRequestBuilderUtil
        .addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_SERVER, 1, true);

    while (_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size() == 0) {
      Thread.sleep(100);
    }

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size(),
        1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size(),
        1);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopController();
    stopZk();
  }

  @Test
  public void testSegmentCrcApi()
      throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(1)
            .build();
    _helixResourceManager.addTable(tableConfig);

    // Wait for the table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    // Check when there is no segment.
    Map<String, SegmentMetadata> segmentMetadataTable = new HashMap<>();
    checkCrcRequest(segmentMetadataTable, 0);

    // Upload Segments
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      _helixResourceManager.addNewSegment(TABLE_NAME, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 5);

    // Add more segments
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME);
      _helixResourceManager.addNewSegment(TABLE_NAME, segmentMetadata, "downloadUrl");
      segmentMetadataTable.put(segmentMetadata.getName(), segmentMetadata);
    }

    // Get crc info from API and check that they are correct.
    checkCrcRequest(segmentMetadataTable, 10);

    // Delete segments
    _helixResourceManager.deleteSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
        segmentMetadataTable.values().iterator().next().getName());

    // Check crc api
    checkCrcRequest(segmentMetadataTable, 9);
  }

  private void checkCrcRequest(Map<String, SegmentMetadata> metadataTable, int expectedSize)
      throws Exception {
    String crcMapStr = sendGetRequest(_controllerRequestURLBuilder.forListAllCrcInformationForTable(TABLE_NAME));
    Map<String, String> crcMap = JsonUtils.stringToObject(crcMapStr, Map.class);
    for (String segmentName : crcMap.keySet()) {
      SegmentMetadata metadata = metadataTable.get(segmentName);
      Assert.assertTrue(metadata != null);
      Assert.assertEquals(crcMap.get(segmentName), metadata.getCrc());
    }
    Assert.assertEquals(crcMap.size(), expectedSize);
  }
}
