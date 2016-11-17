/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.integration.tests.UploadRefreshDeleteIntegrationTest;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for the balance num segment assignment strategy
 */
public class BalanceNumSegmentAssignmentStrategyIntegrationTest extends UploadRefreshDeleteIntegrationTest {

  private ZKHelixAdmin _helixAdmin;

  @BeforeClass
  public void setUp() throws Exception {
    // Start zk and controller
    startZk();
    startController();

    // Start one server and one broker instance
    startServer();
    startBroker();

    // Create eight dummy server instances
    for(int i = 0; i < 8; ++i) {
      JSONObject serverInstance = new JSONObject();
      serverInstance.put("host", "1.2.3.4");
      serverInstance.put("port", Integer.toString(1234 + i));
      serverInstance.put("tag", "DefaultTenant_OFFLINE");
      serverInstance.put("type", "server");
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceCreate(),
          serverInstance.toString());
    }

    // Create Helix connection
    _helixAdmin = new ZKHelixAdmin(ZkStarter.DEFAULT_ZK_STR);

    // BaseClass @BeforeMethod will setup tablex
  }

  @AfterClass
  public void tearDown() {
    // Close the Helix connection
    _helixAdmin.close();

    // Stop everything
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    // Delete temporary directory
    FileUtils.deleteQuietly(_tmpDir);
  }

  @Test(dataProvider = "configProvider")
  public void testSegmentAssignmentStrategy(String tableName, SegmentVersion version) throws Exception {
    // Upload nine segments
    for(int i = 0; i < 9; ++i) {
      generateAndUploadRandomSegment(tableName + "_" + i, 10);
    }

    // Count the number of segments per instance
    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), tableName + "_OFFLINE");
    Map<String, Integer> segmentsPerInstance = new HashMap<String, Integer>();

    for (String partitionName : idealState.getPartitionSet()) {
      for (String instanceName : idealState.getInstanceSet(partitionName)) {
        if (!segmentsPerInstance.containsKey(instanceName)) {
          segmentsPerInstance.put(instanceName, 1);
        } else {
          segmentsPerInstance.put(instanceName, segmentsPerInstance.get(instanceName) + 1);
        }
      }
    }

    // Check that each instance has exactly three segments assigned
    for (String instanceName : segmentsPerInstance.keySet()) {
      int segmentCountForInstance = segmentsPerInstance.get(instanceName);
      assertEquals(segmentCountForInstance, 3,
          "Instance " + instanceName + " did not have the expected number of segments assigned");
    }
  }

  @Test(enabled = false)
  public void testRefresh(String tableName, SegmentVersion version) {
    // Ignore this inherited test
  }

  @Test(enabled = false)
  public void testRetry(String tableName, SegmentVersion version) {
    // Ignore this inherited test
  }
}
