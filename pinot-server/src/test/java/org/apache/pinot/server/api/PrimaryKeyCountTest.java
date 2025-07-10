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
package org.apache.pinot.server.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.restlet.resources.PrimaryKeyCountInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.server.api.resources.PrimaryKeyCount;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PrimaryKeyCountTest {

  @Test
  public void testComputePrimaryKeysForEmptyOrNullInstanceId() {
    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);

    Assert.assertThrows(IllegalArgumentException.class,
        () -> PrimaryKeyCount.computeNumberOfPrimaryKeys("", instanceDataManager));

    Assert.assertThrows(IllegalArgumentException.class,
        () -> PrimaryKeyCount.computeNumberOfPrimaryKeys(null, instanceDataManager));
  }

  @Test
  public void testComputePrimaryKeysForNullInstanceDataManager() {
    Assert.assertThrows(IllegalArgumentException.class,
        () -> PrimaryKeyCount.computeNumberOfPrimaryKeys("instanceId", null));
  }


  @Test
  public void testComputePrimaryKeysNullTableDataManager() {
    Set<String> allTables = new HashSet<>();
    allTables.add("myTable_REALTIME");
    allTables.add("myTable_OFFLINE");

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("myTable_REALTIME")).thenReturn(null);
    when(instanceDataManager.getTableDataManager("myTable_OFFLINE")).thenReturn(null);
    when(instanceDataManager.getAllTables()).thenReturn(allTables);

    String instanceId = "instance42";
    PrimaryKeyCountInfo primaryKeyCountInfo = PrimaryKeyCount.computeNumberOfPrimaryKeys(instanceId,
        instanceDataManager);

    Assert.assertEquals(primaryKeyCountInfo.getInstanceId(), instanceId);
    Assert.assertEquals(primaryKeyCountInfo.getNumPrimaryKeys(), 0);
    Assert.assertNotNull(primaryKeyCountInfo.getUpsertAndDedupTables());
    Assert.assertEquals(primaryKeyCountInfo.getUpsertAndDedupTables().size(), 0);
    Assert.assertTrue(primaryKeyCountInfo.getLastUpdatedTimeInEpochMs() <= System.currentTimeMillis());
  }

  @Test
  public void testComputePrimaryKeysNonUpsertDedupTables() {
    Set<String> allTables = new HashSet<>();
    allTables.add("myTable_REALTIME");
    allTables.add("myTable_OFFLINE");

    // Mock the Table data manager
    RealtimeTableDataManager realtimeTableDataManager = mock(RealtimeTableDataManager.class);
    when(realtimeTableDataManager.isUpsertEnabled()).thenReturn(false);
    when(realtimeTableDataManager.isDedupEnabled()).thenReturn(false);
    OfflineTableDataManager offlineTableDataManager = mock(OfflineTableDataManager.class);
    when(realtimeTableDataManager.getTableUpsertMetadataManager()).thenReturn(null);
    when(realtimeTableDataManager.getTableDedupMetadataManager()).thenReturn(null);

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("myTable_REALTIME"))
        .thenReturn(realtimeTableDataManager);
    when(instanceDataManager.getTableDataManager("myTable_OFFLINE"))
        .thenReturn(offlineTableDataManager);
    when(instanceDataManager.getAllTables()).thenReturn(allTables);

    String instanceId = "instance42";
    PrimaryKeyCountInfo primaryKeyCountInfo = PrimaryKeyCount.computeNumberOfPrimaryKeys(instanceId,
        instanceDataManager);

    Assert.assertEquals(primaryKeyCountInfo.getInstanceId(), instanceId);
    Assert.assertEquals(primaryKeyCountInfo.getNumPrimaryKeys(), 0);
    Assert.assertNotNull(primaryKeyCountInfo.getUpsertAndDedupTables());
    Assert.assertEquals(primaryKeyCountInfo.getUpsertAndDedupTables().size(), 0);
    Assert.assertTrue(primaryKeyCountInfo.getLastUpdatedTimeInEpochMs() <= System.currentTimeMillis());
  }

  @Test
  public void testComputePrimaryKeysUpsertOnly() {
    Set<String> allTables = new HashSet<>();
    allTables.add("myTable_REALTIME");
    allTables.add("myTable_OFFLINE");

    Map<Integer, Long> partitionToPrimaryKeyCountMap = new HashMap<>();
    partitionToPrimaryKeyCountMap.put(0, 42L);
    partitionToPrimaryKeyCountMap.put(1, 420L);
    partitionToPrimaryKeyCountMap.put(2, 4200L);

    long primaryKeyCount = 0;
    for (Long count : partitionToPrimaryKeyCountMap.values()) {
      primaryKeyCount += count;
    }

    // Mock the Table data manager
    RealtimeTableDataManager realtimeTableDataManager = mock(RealtimeTableDataManager.class);
    when(realtimeTableDataManager.isUpsertEnabled()).thenReturn(true);
    when(realtimeTableDataManager.isDedupEnabled()).thenReturn(false);
    OfflineTableDataManager offlineTableDataManager = mock(OfflineTableDataManager.class);

    // Mock the upsert manager
    TableUpsertMetadataManager tableUpsertMetadataManager = mock(TableUpsertMetadataManager.class);
    when(tableUpsertMetadataManager.getPartitionToPrimaryKeyCount()).thenReturn(partitionToPrimaryKeyCountMap);
    when(realtimeTableDataManager.getTableUpsertMetadataManager()).thenReturn(tableUpsertMetadataManager);

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("myTable_REALTIME"))
        .thenReturn(realtimeTableDataManager);
    when(instanceDataManager.getTableDataManager("myTable_OFFLINE"))
        .thenReturn(offlineTableDataManager);
    when(instanceDataManager.getAllTables()).thenReturn(allTables);

    String instanceId = "instance42";
    PrimaryKeyCountInfo primaryKeyCountInfo = PrimaryKeyCount.computeNumberOfPrimaryKeys(instanceId,
        instanceDataManager);

    Assert.assertEquals(primaryKeyCountInfo.getInstanceId(), instanceId);
    Assert.assertEquals(primaryKeyCountInfo.getNumPrimaryKeys(), primaryKeyCount);
    Assert.assertNotNull(primaryKeyCountInfo.getUpsertAndDedupTables());
    Assert.assertEquals(primaryKeyCountInfo.getUpsertAndDedupTables().size(), 1);
    Assert.assertTrue(primaryKeyCountInfo.getUpsertAndDedupTables().contains("myTable_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo.getLastUpdatedTimeInEpochMs() <= System.currentTimeMillis());
  }

  @Test
  public void testComputePrimaryKeysDedupOnly() {
    Set<String> allTables = new HashSet<>();
    allTables.add("myTable_REALTIME");
    allTables.add("myTable_OFFLINE");

    Map<Integer, Long> partitionToPrimaryKeyCountMap = new HashMap<>();
    partitionToPrimaryKeyCountMap.put(0, 42L);
    partitionToPrimaryKeyCountMap.put(1, 420L);
    partitionToPrimaryKeyCountMap.put(2, 4200L);

    long primaryKeyCount = 0;
    for (Long count : partitionToPrimaryKeyCountMap.values()) {
      primaryKeyCount += count;
    }

    // Mock the Table data manager
    RealtimeTableDataManager realtimeTableDataManager = mock(RealtimeTableDataManager.class);
    when(realtimeTableDataManager.isUpsertEnabled()).thenReturn(false);
    when(realtimeTableDataManager.isDedupEnabled()).thenReturn(true);
    OfflineTableDataManager offlineTableDataManager = mock(OfflineTableDataManager.class);

    // Mock the dedup manager
    TableDedupMetadataManager tableDedupMetadataManager = mock(TableDedupMetadataManager.class);
    when(tableDedupMetadataManager.getPartitionToPrimaryKeyCount()).thenReturn(partitionToPrimaryKeyCountMap);
    when(realtimeTableDataManager.getTableDedupMetadataManager()).thenReturn(tableDedupMetadataManager);

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("myTable_REALTIME"))
        .thenReturn(realtimeTableDataManager);
    when(instanceDataManager.getTableDataManager("myTable_OFFLINE"))
        .thenReturn(offlineTableDataManager);
    when(instanceDataManager.getAllTables()).thenReturn(allTables);

    String instanceId = "instance42";
    PrimaryKeyCountInfo primaryKeyCountInfo = PrimaryKeyCount.computeNumberOfPrimaryKeys(instanceId,
        instanceDataManager);

    Assert.assertEquals(primaryKeyCountInfo.getInstanceId(), instanceId);
    Assert.assertEquals(primaryKeyCountInfo.getNumPrimaryKeys(), primaryKeyCount);
    Assert.assertNotNull(primaryKeyCountInfo.getUpsertAndDedupTables());
    Assert.assertEquals(primaryKeyCountInfo.getUpsertAndDedupTables().size(), 1);
    Assert.assertTrue(primaryKeyCountInfo.getUpsertAndDedupTables().contains("myTable_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo.getLastUpdatedTimeInEpochMs() <= System.currentTimeMillis());
  }

  @Test
  public void testComputePrimaryKeysUpsertAndDedup() {
    Set<String> allTables = new HashSet<>();
    allTables.add("myTableUpsert_REALTIME");
    allTables.add("myTableDedup_REALTIME");
    allTables.add("myTable_OFFLINE");

    Map<Integer, Long> upsertPartitionToPrimaryKeyCountMap = new HashMap<>();
    upsertPartitionToPrimaryKeyCountMap.put(0, 42L);
    upsertPartitionToPrimaryKeyCountMap.put(1, 420L);
    upsertPartitionToPrimaryKeyCountMap.put(2, 4200L);

    Map<Integer, Long> dedupPartitionToPrimaryKeyCountMap = new HashMap<>();
    dedupPartitionToPrimaryKeyCountMap.put(0, 2042L);
    dedupPartitionToPrimaryKeyCountMap.put(1, 777L);

    long primaryKeyCount = 0;
    for (Long count : upsertPartitionToPrimaryKeyCountMap.values()) {
      primaryKeyCount += count;
    }
    for (Long count : dedupPartitionToPrimaryKeyCountMap.values()) {
      primaryKeyCount += count;
    }

    // Mock the Table data manager
    RealtimeTableDataManager upsertRealtimeTableDataManager = mock(RealtimeTableDataManager.class);
    when(upsertRealtimeTableDataManager.isUpsertEnabled()).thenReturn(true);
    when(upsertRealtimeTableDataManager.isDedupEnabled()).thenReturn(false);
    RealtimeTableDataManager dedupRealtimeTableDataManager = mock(RealtimeTableDataManager.class);
    when(dedupRealtimeTableDataManager.isUpsertEnabled()).thenReturn(false);
    when(dedupRealtimeTableDataManager.isDedupEnabled()).thenReturn(true);
    OfflineTableDataManager offlineTableDataManager = mock(OfflineTableDataManager.class);

    // Mock the upsert manager
    TableUpsertMetadataManager tableUpsertMetadataManager = mock(TableUpsertMetadataManager.class);
    when(tableUpsertMetadataManager.getPartitionToPrimaryKeyCount()).thenReturn(upsertPartitionToPrimaryKeyCountMap);
    when(upsertRealtimeTableDataManager.getTableUpsertMetadataManager()).thenReturn(tableUpsertMetadataManager);

    // Mock the dedup manager
    TableDedupMetadataManager tableDedupMetadataManager = mock(TableDedupMetadataManager.class);
    when(tableDedupMetadataManager.getPartitionToPrimaryKeyCount()).thenReturn(dedupPartitionToPrimaryKeyCountMap);
    when(dedupRealtimeTableDataManager.getTableDedupMetadataManager()).thenReturn(tableDedupMetadataManager);

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("myTableUpsert_REALTIME"))
        .thenReturn(upsertRealtimeTableDataManager);
    when(instanceDataManager.getTableDataManager("myTableDedup_REALTIME"))
        .thenReturn(dedupRealtimeTableDataManager);
    when(instanceDataManager.getTableDataManager("myTable_OFFLINE"))
        .thenReturn(offlineTableDataManager);
    when(instanceDataManager.getAllTables()).thenReturn(allTables);

    String instanceId = "instance42";
    PrimaryKeyCountInfo primaryKeyCountInfo = PrimaryKeyCount.computeNumberOfPrimaryKeys(instanceId,
        instanceDataManager);

    Assert.assertEquals(primaryKeyCountInfo.getInstanceId(), instanceId);
    Assert.assertEquals(primaryKeyCountInfo.getNumPrimaryKeys(), primaryKeyCount);
    Assert.assertNotNull(primaryKeyCountInfo.getUpsertAndDedupTables());
    Assert.assertEquals(primaryKeyCountInfo.getUpsertAndDedupTables().size(), 2);
    Assert.assertTrue(primaryKeyCountInfo.getUpsertAndDedupTables().contains("myTableUpsert_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo.getUpsertAndDedupTables().contains("myTableDedup_REALTIME"));
    Assert.assertTrue(primaryKeyCountInfo.getLastUpdatedTimeInEpochMs() <= System.currentTimeMillis());
  }
}
