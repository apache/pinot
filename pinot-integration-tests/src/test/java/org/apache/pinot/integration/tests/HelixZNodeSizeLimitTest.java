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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * This test is created to show the bug in Helix 0.9.8 that if a ZooKeeper IdealState is larger than 1MB after
 * compression, it cannot be updated anymore. Somehow this test can also make sure in future we will support
 * large IdealStates
 */
@Test(suiteName = "integration-suite-1", groups = {"integration-suite-1"})
public class HelixZNodeSizeLimitTest extends BaseClusterIntegrationTest {
  @BeforeSuite
  public void setUpSuite() {
    // This line of code has to be executed before the org.apache.helix.zookeeper.zkclient.ZkClient.WRITE_SIZE_LIMIT
    // is initialized. The code is in:
    // https://github.com/apache/helix/blob/master/zookeeper-api/
    // src/main/java/org/apache/helix/zookeeper/zkclient/ZkClient.java#L105
    // The below line gets executed before ZkClient.WRITE_SIZE_LIMIT is created
    System.setProperty(ZkSystemPropertyKeys.JUTE_MAXBUFFER, "4000000");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    System.out.println("setUp this.getClass().getName() = " + this.getClass().getName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start Zookeeper
    startZk();
    startController();
    startBroker();
    startServer();
    addSchema(createSchema());
    addTableConfig(createOfflineTableConfig());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    System.out.println("tearDown this.getClass().getName() = " + this.getClass().getName());
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Test
  public void testUpdateIdealState() {
    // In Helix 0.9.8, we get error logs like below:
    // 13:03:51.576 ERROR [ZkBaseDataAccessor] [main] Exception while setting path:
    // /HelixZNodeSizeLimitTest/IDEALSTATES/mytable_OFFLINE
    //  org.apache.helix.HelixException: Data size larger than 1M
    //  at org.apache.helix.manager.zk.zookeeper.ZkClient.checkDataSizeLimit(ZkClient.java:1513) ~[helix-core-0.9.8
    //  .jar:0.9.8]
    //  at org.apache.helix.manager.zk.zookeeper.ZkClient.writeDataReturnStat(ZkClient.java:1406) ~[helix-core-0.9.8
    //  .jar:0.9.8]
    String tableNameWithType = getTableName() + "_OFFLINE";
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES, "4000000");
    // The updated IdealState after compression is roughly 2MB
    try {
      HelixHelper.updateIdealState(_helixManager, tableNameWithType, idealState -> {
        Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
        for (int i = 0; i < 500_000; i++) {
          currentAssignment.put("segment_" + i,
              ImmutableMap.of("Server_with_some_reasonable_long_prefix_" + (i % 10), "ONLINE"));
          currentAssignment.put("segment_" + i,
              ImmutableMap.of("Server_with_some_reasonable_long_prefix_" + (i % 9), "ONLINE"));
        }
        return idealState;
      });
    } catch (Exception e) {
      Assert.fail("Exception shouldn't be thrown even if the data size of the ideal state is larger than 1M");
    } finally {
      System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    }
  }
}
