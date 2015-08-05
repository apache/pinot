/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;


public class PinotResourceManagerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotResourceManagerTest.class);

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestPinotResourceManager";
  private final static String TABLE_NAME = "testTable";
  private ZkClient _zkClient;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  @BeforeTest
  public void setup() throws Exception {
    ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZK_SERVER);

    final String instanceId = "localhost_helixController";
    _pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true);
    _pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    /////////////////////////
    _numInstance = 1;
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER,
        _numInstance, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, 1, true);
    Thread.sleep(3000);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 1);
    Assert
        .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_REALTIME").size(),
        1);

    // Adding table
    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(TABLE_NAME, null, null, 1).toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);

  }

  @AfterTest
  public void tearDown() {
    _pinotHelixResourceManager.stop();
    _zkClient.close();
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testAddingAndDeletingSegments() throws Exception {
    for (int i = 1; i <= 5; i++) {
      addOneSegment(TABLE_NAME);
      Thread.sleep(2000);
      final ExternalView externalView =
          _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME,
              TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME));
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
    final ExternalView externalView =
        _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME,
            TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME));
    int i = 4;
    for (final String segmentId : externalView.getPartitionSet()) {
      deleteOneSegment(TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME), segmentId);
      Thread.sleep(2000);
      Assert.assertEquals(
          _helixAdmin
              .getResourceExternalView(HELIX_CLUSTER_NAME,
                  TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME)).getPartitionSet().size(), i);
      i--;
    }
  }

  /**
   * Creates 5 threads that concurrently try to add 20 segments each, and asserts that we have
   * 100 segments in the end. Then launches 5 threads again that concurrently try to delete all segments,
   * and makes sure that we have zero segments left in the end.
   * @throws Exception
   */

  @Test
  public void testConcurrentAddingAndDeletingSegments() throws Exception {
    ExecutorService addSegmentExecutor = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 5; ++i) {
      addSegmentExecutor.execute(new Runnable() {

        @Override
        public void run() {
          for (int i = 0; i < 20; ++i) {
            addOneSegment(TABLE_NAME);
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Assert.assertFalse(true, "Exception caught during sleep.");
            }
          }
        }
      });
    }
    addSegmentExecutor.shutdown();
    while (!addSegmentExecutor.isTerminated()) {
    }

    final String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME);
    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 100);

    ExecutorService deleteSegmentExecutor = Executors.newFixedThreadPool(5);
    for (final String segment : idealState.getPartitionSet()) {
      deleteSegmentExecutor.execute(new Runnable() {

        @Override
        public void run() {
          deleteOneSegment(offlineTableName, segment);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Assert.assertFalse(true, "Exception caught during sleep.");
          }
        }
      });
    }
    deleteSegmentExecutor.shutdown();
    while (!deleteSegmentExecutor.isTerminated()) {
    }

    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);
  }

  public void testWithCmdLines() throws Exception {

    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      final String command = br.readLine();
      if ((command != null) && command.equals("exit")) {
        tearDown();
      }
      if ((command != null) && command.equals("add")) {
        addOneSegment(TABLE_NAME);
      }
      if ((command != null) && command.startsWith("delete")) {
        final String segment2delete = command.split(" ")[1];
        deleteOneSegment(TABLE_NAME, segment2delete);
      }
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotHelixResourceManager.addSegment(segmentMetadata, "downloadUrl");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotHelixResourceManager.deleteSegment(resource, segment);
  }

}
