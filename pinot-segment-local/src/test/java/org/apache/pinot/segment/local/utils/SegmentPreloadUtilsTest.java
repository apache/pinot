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
package org.apache.pinot.segment.local.utils;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentPreloadUtilsTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentPreloadUtilsTest");

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.forceDelete(TEMP_DIR);
  }

  @Test
  public void testPreloadSegments()
      throws Exception {
    String realtimeTableName = "testTable_REALTIME";
    String instanceId = "server01";
    Map<String, Map<String, String>> segmentAssignment = new HashMap<>();
    Map<String, SegmentZKMetadata> segmentMetadataMap = new HashMap<>();
    TableDataManager tableDataManager = mock(TableDataManager.class);

    // Setup mocks for TableConfig and Schema.
    TableConfig tableConfig = mock(TableConfig.class);
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setComparisonColumn("ts");
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setPreload(Enablement.ENABLE);
    when(tableConfig.getUpsertConfig()).thenReturn(upsertConfig);
    when(tableConfig.getTableName()).thenReturn(realtimeTableName);
    Schema schema = mock(Schema.class);
    when(schema.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("pk"));
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.getTableConfig()).thenReturn(tableConfig);

    // Setup mocks for HelixManager.
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    // Setup segment assignment. Only ONLINE segments are preloaded.
    segmentAssignment.put("consuming_seg01", ImmutableMap.of(instanceId, "CONSUMING"));
    segmentAssignment.put("consuming_seg02", ImmutableMap.of(instanceId, "CONSUMING"));
    segmentAssignment.put("offline_seg01", ImmutableMap.of(instanceId, "OFFLINE"));
    segmentAssignment.put("offline_seg02", ImmutableMap.of(instanceId, "OFFLINE"));
    String seg01Name = "testTable__0__1__" + System.currentTimeMillis();
    segmentAssignment.put(seg01Name, ImmutableMap.of(instanceId, "ONLINE"));
    String seg02Name = "testTable__0__2__" + System.currentTimeMillis();
    segmentAssignment.put(seg02Name, ImmutableMap.of(instanceId, "ONLINE"));
    // This segment is skipped as it's not from partition 0.
    String seg03Name = "testTable__1__3__" + System.currentTimeMillis();
    segmentAssignment.put(seg03Name, ImmutableMap.of(instanceId, "ONLINE"));

    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(seg01Name);
    zkMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentMetadataMap.put(seg01Name, zkMetadata);
    zkMetadata = new SegmentZKMetadata(seg02Name);
    zkMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentMetadataMap.put(seg02Name, zkMetadata);
    zkMetadata = new SegmentZKMetadata(seg03Name);
    zkMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentMetadataMap.put(seg03Name, zkMetadata);

    // Setup mocks to get file path to validDocIds snapshot.
    ExecutorService segmentPreloadExecutor = Executors.newFixedThreadPool(1);
    File tableDataDir = new File(TEMP_DIR, realtimeTableName);
    when(tableDataManager.getHelixManager()).thenReturn(helixManager);
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(segmentPreloadExecutor);
    when(tableDataManager.getTableDataDir()).thenReturn(tableDataDir);
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceId()).thenReturn(instanceId);
    when(tableDataManager.getInstanceDataManagerConfig()).thenReturn(instanceDataManagerConfig);

    // No snapshot file for seg01, so it's skipped.
    File seg01IdxDir = new File(tableDataDir, seg01Name);
    FileUtils.forceMkdir(seg01IdxDir);
    when(tableDataManager.getSegmentDataDir(seg01Name, null, tableConfig)).thenReturn(seg01IdxDir);

    File seg02IdxDir = new File(tableDataDir, seg02Name);
    FileUtils.forceMkdir(seg02IdxDir);
    FileUtils.touch(new File(new File(seg02IdxDir, "v3"), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));
    when(tableDataManager.getSegmentDataDir(seg02Name, null, tableConfig)).thenReturn(seg02IdxDir);

    try {
      List<String> preloadedSegments =
          SegmentPreloadUtils.doPreloadSegments(tableDataManager, 0, indexLoadingConfig, segmentAssignment,
              segmentMetadataMap, segmentPreloadExecutor,
              (segmentName, segmentZKMetadata) -> SegmentPreloadUtils.hasValidDocIdsSnapshot(tableDataManager,
                  tableConfig, segmentName, segmentZKMetadata.getTier()));
      assertEquals(preloadedSegments.size(), 1);
      assertTrue(preloadedSegments.contains(seg02Name));
    } finally {
      segmentPreloadExecutor.shutdownNow();
    }
  }
}
