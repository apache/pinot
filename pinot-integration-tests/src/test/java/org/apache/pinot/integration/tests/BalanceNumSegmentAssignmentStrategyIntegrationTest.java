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
package org.apache.pinot.integration.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.config.Instance;
import org.apache.pinot.common.utils.CommonConstants.Helix.InstanceType;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for the balance num segment assignment strategy
 */
// TODO: clean up this test
public class BalanceNumSegmentAssignmentStrategyIntegrationTest extends UploadRefreshDeleteIntegrationTest {
  private static final String HOST = "1.2.3.4";
  private static final int BASE_PORT = 1234;
  private static final String SERVER_TAG = "DefaultTenant_OFFLINE";

  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();

    // Create eight dummy server instances
    for (int i = 0; i < 8; ++i) {
      Instance serverInstance =
          new Instance(HOST, BASE_PORT + i, InstanceType.SERVER, Collections.singletonList(SERVER_TAG), null);
      sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toJsonString());
    }
  }

  @DataProvider(name = "tableNameProvider")
  public Object[][] configProvider() {
    Object[][] configs = {{"disabledInstancesTable", SegmentVersion.v3}};
    return configs;
  }

  @Test(dataProvider = "tableNameProvider")
  public void testSegmentAssignmentStrategy(String tableName, SegmentVersion version)
      throws Exception {
    // Upload nine segments
    for (int i = 0; i < 9; ++i) {
      generateAndUploadRandomSegment(tableName + "_" + i, 10);
    }

    // Count the number of segments per instance
    Map<String, Integer> segmentsPerInstance = getSegmentsPerInstance(tableName);
    // Check that each instance has exactly three segments assigned
    for (String instanceName : segmentsPerInstance.keySet()) {
      int segmentCountForInstance = segmentsPerInstance.get(instanceName);
      assertEquals(segmentCountForInstance, 3,
          "Instance " + instanceName + " did not have the expected number of segments assigned");
    }
  }

  private Map<String, Integer> getSegmentsPerInstance(String tableName) {
    Map<String, Integer> segmentsPerInstance = new HashMap<String, Integer>();
    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), tableName + "_OFFLINE");
    for (String partitionName : idealState.getPartitionSet()) {
      for (String instanceName : idealState.getInstanceSet(partitionName)) {
        if (!segmentsPerInstance.containsKey(instanceName)) {
          segmentsPerInstance.put(instanceName, 1);
        } else {
          segmentsPerInstance.put(instanceName, segmentsPerInstance.get(instanceName) + 1);
        }
      }
    }
    return segmentsPerInstance;
  }

  @Test(dataProvider = "tableNameProvider")
  public void testNoAssignmentToDisabledInstances(String tableName, SegmentVersion version)
      throws Exception {
    List<String> instances = _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), SERVER_TAG);
    List<String> disabledInstances = new ArrayList<>();
    // disable 6 instances
    assertEquals(instances.size(), 9);
    for (int i = 0; i < 6; i++) {
      _helixAdmin.enableInstance(getHelixClusterName(), instances.get(i), false);
    }

    // Thread.sleep(100000);
    for (int i = 0; i < 6; i++) {
      generateAndUploadRandomSegment(tableName + "_" + i, 10);
    }

    Map<String, Integer> segmentsPerInstance = getSegmentsPerInstance(tableName);
    // size is 3 since we disabled 6 instances
    assertEquals(segmentsPerInstance.size(), 3);
    for (Map.Entry<String, Integer> instanceEntry : segmentsPerInstance.entrySet()) {
      assertEquals(instanceEntry.getValue().intValue(), 6);
    }

    // re-enable instances since these tests are usually "setup once" and run multiple tests type
    for (int i = 0; i < 6; i++) {
      _helixAdmin.enableInstance(getHelixClusterName(), instances.get(i), true);
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
