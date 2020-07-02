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
package org.apache.pinot.controller.helix;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotResourceManagerTest extends ControllerTest {
  private static final String TABLE_NAME = "testTable";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size(),
        1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size(),
        1);
    Assert
        .assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_REALTIME").size(),
            1);

    // Adding table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    _helixResourceManager.addTable(tableConfig);
  }

  @Test
  public void testUpdateSegmentZKMetadata() {
    OfflineSegmentZKMetadata segmentZKMetadata = new OfflineSegmentZKMetadata();
    segmentZKMetadata.setSegmentName("testSegment");

    // Segment ZK metadata does not exist
    Assert.assertFalse(_helixResourceManager.updateZkMetadata("testTable_OFFLINE", segmentZKMetadata, 0));

    // Set segment ZK metadata
    Assert.assertTrue(_helixResourceManager.updateZkMetadata("testTable_OFFLINE", segmentZKMetadata));

    // Update ZK metadata
    Assert
        .assertEquals(_helixResourceManager.getSegmentMetadataZnRecord("testTable_OFFLINE", "testSegment").getVersion(),
            0);
    Assert.assertTrue(_helixResourceManager.updateZkMetadata("testTable_OFFLINE", segmentZKMetadata, 0));
    Assert
        .assertEquals(_helixResourceManager.getSegmentMetadataZnRecord("testTable_OFFLINE", "testSegment").getVersion(),
            1);
    Assert.assertFalse(_helixResourceManager.updateZkMetadata("testTable_OFFLINE", segmentZKMetadata, 0));
  }

  /**
   * First tests basic segment adding/deleting.
   * Then creates 3 threads that concurrently try to add 10 segments each, and asserts that we have
   * 100 segments in the end. Then launches 5 threads again that concurrently try to delete all segments,
   * and makes sure that we have zero segments left in the end.
   * @throws Exception
   */

  @Test
  public void testBasicAndConcurrentAddingAndDeletingSegments()
      throws Exception {
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);

    // Basic add/delete case
    for (int i = 1; i <= 2; i++) {
      _helixResourceManager
          .addNewSegment(TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME), "downloadUrl");
    }
    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), offlineTableName);
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 2);

    for (String segmentName : segments) {
      _helixResourceManager.deleteSegment(offlineTableName, segmentName);
    }
    idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);

    // Concurrent segment deletion
    ExecutorService addSegmentExecutor = Executors.newFixedThreadPool(3);
    for (int i = 0; i < 3; ++i) {
      addSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < 10; i++) {
            _helixResourceManager
                .addNewSegment(TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME), "downloadUrl");
          }
        }
      });
    }
    addSegmentExecutor.shutdown();
    addSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 30);

    ExecutorService deleteSegmentExecutor = Executors.newFixedThreadPool(3);
    for (final String segmentName : idealState.getPartitionSet()) {
      deleteSegmentExecutor.execute(new Runnable() {
        @Override
        public void run() {
          _helixResourceManager.deleteSegment(offlineTableName, segmentName);
        }
      });
    }
    deleteSegmentExecutor.shutdown();
    deleteSegmentExecutor.awaitTermination(1, TimeUnit.MINUTES);

    idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), offlineTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 0);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
