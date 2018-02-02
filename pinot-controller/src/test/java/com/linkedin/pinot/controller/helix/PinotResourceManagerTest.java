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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.utils.SegmentMetadataMockUtils;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotResourceManagerTest {
  private final static String HELIX_CLUSTER_NAME = "testCluster";
  private final static String TABLE_NAME = "testTable";

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ZkClient _zkClient;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private HelixAdmin _helixAdmin;

  @BeforeClass
  public void setUp() throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);

    final String instanceId = "localhost_helixController";
    _pinotHelixResourceManager =
        new PinotHelixResourceManager(ZkStarter.DEFAULT_ZK_STR, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true,
            /*isUpdateStateModel=*/ false);
    _pinotHelixResourceManager.start();
    _helixAdmin = _pinotHelixResourceManager.getHelixAdmin();

    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 1, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 1, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size(),
        1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_REALTIME").size(),
        1);

    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME).build();
    _pinotHelixResourceManager.addTable(tableConfig);
  }

  @Test
  public void testUpdateSegmentZKMetadata() {
    OfflineSegmentZKMetadata segmentZKMetadata = new OfflineSegmentZKMetadata();
    segmentZKMetadata.setTableName("testTable");
    segmentZKMetadata.setSegmentName("testSegment");

    // Segment ZK metadata does not exist
    Assert.assertFalse(_pinotHelixResourceManager.updateZkMetadata(segmentZKMetadata, 0));

    // Set segment ZK metadata
    Assert.assertTrue(_pinotHelixResourceManager.updateZkMetadata(segmentZKMetadata));

    // Update ZK metadata
    Assert.assertEquals(
        _pinotHelixResourceManager.getSegmentMetadataZnRecord("testTable_OFFLINE", "testSegment").getVersion(), 0);
    Assert.assertTrue(_pinotHelixResourceManager.updateZkMetadata(segmentZKMetadata, 0));
    Assert.assertEquals(
        _pinotHelixResourceManager.getSegmentMetadataZnRecord("testTable_OFFLINE", "testSegment").getVersion(), 1);
    Assert.assertFalse(_pinotHelixResourceManager.updateZkMetadata(segmentZKMetadata, 0));
  }

  /**
   * First tests basic segment adding/deleting.
   * Then creates 3 threads that concurrently try to add 10 segments each, and asserts that we have
   * 100 segments in the end. Then launches 5 threads again that concurrently try to delete all segments,
   * and makes sure that we have zero segments left in the end.
   * @throws Exception
   */

  @Test
  public void testBasicAndConcurrentAddingAndDeletingSegments() throws Exception {
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);

    // Basic add/delete case
    for (int i = 1; i <= 2; i++) {
      _pinotHelixResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME), "downloadUrl");
    }
    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 2);

    for (String segmentName : segments) {
      _pinotHelixResourceManager.deleteSegment(offlineTableName, segmentName);
    }
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);

    // Concurrent segment deletion
    ExecutorService addSegmentExecutor = Executors.newFixedThreadPool(3);
    for (int i = 0; i < 3; ++i) {
      addSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < 10; i++) {
            _pinotHelixResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME),
                "downloadUrl");
          }
        }
      });
    }
    addSegmentExecutor.shutdown();
    addSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 30);

    ExecutorService deleteSegmentExecutor = Executors.newFixedThreadPool(3);
    for (final String segmentName : idealState.getPartitionSet()) {
      deleteSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          _pinotHelixResourceManager.deleteSegment(offlineTableName, segmentName);
        }
      });
    }
    deleteSegmentExecutor.shutdown();
    deleteSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);
  }

  @AfterClass
  public void tearDown() {
    _pinotHelixResourceManager.stop();
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }
}
