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
package org.apache.pinot.segment.local.upsert;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ConcurrentMapTableUpsertMetadataManagerTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "ConcurrentMapTableUpsertMetadataManagerTest");
  private ExecutorService _segmentPreloadExecutor;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    _segmentPreloadExecutor = Executors.newFixedThreadPool(1);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
    _segmentPreloadExecutor.shutdownNow();
  }

  @Test
  public void testSkipPreloadSegments() {
    TableConfig tableConfig = mock(TableConfig.class);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setComparisonColumn("ts");
    when(tableConfig.getUpsertConfig()).thenReturn(upsertConfig);
    Schema schema = mock(Schema.class);
    when(schema.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("pk"));

    // Preloading is skipped as snapshot is not enabled.
    ConcurrentMapTableUpsertMetadataManager mgr = new ConcurrentMapTableUpsertMetadataManager();
    assertFalse(mgr.isPreloading());
    mgr.init(tableConfig, schema, mock(TableDataManager.class), mock(ServerMetrics.class), mock(HelixManager.class),
        _segmentPreloadExecutor);
    assertFalse(mgr.isPreloading());

    // Preloading is skipped as preloading is not turned on.
    upsertConfig.setEnableSnapshot(true);
    mgr = new ConcurrentMapTableUpsertMetadataManager();
    assertFalse(mgr.isPreloading());
    mgr.init(tableConfig, schema, mock(TableDataManager.class), mock(ServerMetrics.class), mock(HelixManager.class),
        _segmentPreloadExecutor);
    assertFalse(mgr.isPreloading());

    upsertConfig.setEnablePreload(true);
    mgr = new ConcurrentMapTableUpsertMetadataManager();
    assertFalse(mgr.isPreloading());
    // The preloading logic will hit on error as the HelixManager mock is not fully setup. But failure of preloading
    // should not fail the init() method.
    mgr.init(tableConfig, schema, mock(TableDataManager.class), mock(ServerMetrics.class), mock(HelixManager.class),
        _segmentPreloadExecutor);
    assertFalse(mgr.isPreloading());
  }

  @Test
  public void testPreloadOnlineSegments()
      throws Exception {
    String instanceId = "server01";
    Map<String, Map<String, String>> segmentAssignment = new HashMap<>();
    Set<String> preloadedSegments = new HashSet<>();
    AtomicBoolean wasPreloading = new AtomicBoolean(false);
    ConcurrentMapTableUpsertMetadataManager mgr = new ConcurrentMapTableUpsertMetadataManager() {

      @Override
      String getInstanceId() {
        return instanceId;
      }

      @Override
      IndexLoadingConfig createIndexLoadingConfig() {
        return mock(IndexLoadingConfig.class);
      }

      @Override
      Map<String, Map<String, String>> getSegmentAssignment() {
        return segmentAssignment;
      }

      @Override
      void preloadSegmentWithSnapshot(String segmentName, IndexLoadingConfig indexLoadingConfig,
          SegmentZKMetadata zkMetadata) {
        wasPreloading.set(isPreloading());
        preloadedSegments.add(segmentName);
      }
    };

    // Setup mocks for TableConfig and Schema.
    String tableNameWithType = "myTable_REALTIME";
    TableConfig tableConfig = mock(TableConfig.class);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setComparisonColumn("ts");
    upsertConfig.setEnablePreload(true);
    upsertConfig.setEnableSnapshot(true);
    when(tableConfig.getUpsertConfig()).thenReturn(upsertConfig);
    when(tableConfig.getTableName()).thenReturn(tableNameWithType);
    Schema schema = mock(Schema.class);
    when(schema.getPrimaryKeyColumns()).thenReturn(Collections.singletonList("pk"));

    // Setup mocks for HelixManager.
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    // Setup segment assignment. Only ONLINE segments are preloaded.
    segmentAssignment.put("consuming_seg01", ImmutableMap.of(instanceId, "CONSUMING"));
    segmentAssignment.put("consuming_seg02", ImmutableMap.of(instanceId, "CONSUMING"));
    segmentAssignment.put("online_seg01", ImmutableMap.of(instanceId, "ONLINE"));
    segmentAssignment.put("online_seg02", ImmutableMap.of(instanceId, "ONLINE"));
    segmentAssignment.put("offline_seg01", ImmutableMap.of(instanceId, "OFFLINE"));
    segmentAssignment.put("offline_seg02", ImmutableMap.of(instanceId, "OFFLINE"));

    // Setup mocks to get file path to validDocIds snapshot.
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata("online_seg01");
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    when(propertyStore.get(
        eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, "online_seg01")), any(),
        anyInt())).thenReturn(realtimeSegmentZKMetadata.toZNRecord());
    realtimeSegmentZKMetadata = new SegmentZKMetadata("online_seg02");
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    when(propertyStore.get(
        eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, "online_seg02")), any(),
        anyInt())).thenReturn(realtimeSegmentZKMetadata.toZNRecord());

    // No snapshot file for online_seg01, so it's skipped.
    TableDataManager tableDataManager = mock(TableDataManager.class);
    File seg01IdxDir = new File(TEMP_DIR, "online_seg01");
    FileUtils.forceMkdir(seg01IdxDir);
    when(tableDataManager.getSegmentDataDir("online_seg01", null, tableConfig)).thenReturn(seg01IdxDir);

    File seg02IdxDir = new File(TEMP_DIR, "online_seg02");
    FileUtils.forceMkdir(seg02IdxDir);
    FileUtils.touch(new File(new File(seg02IdxDir, "v3"), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));
    when(tableDataManager.getSegmentDataDir("online_seg02", null, tableConfig)).thenReturn(seg02IdxDir);

    assertFalse(mgr.isPreloading());
    mgr.init(tableConfig, schema, tableDataManager, mock(ServerMetrics.class), helixManager, _segmentPreloadExecutor);
    assertEquals(preloadedSegments.size(), 1);
    assertTrue(preloadedSegments.contains("online_seg02"));
    assertTrue(wasPreloading.get());
    assertFalse(mgr.isPreloading());
  }
}
