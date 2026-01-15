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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class PinotInstanceAssignmentRestletResourceTest {
  @Mock
  PinotHelixResourceManager _resourceManager;

  @InjectMocks
  PinotInstanceAssignmentRestletResource _resource;

  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  private static ZNRecord createInstancePartitionsZN(String id, List<String> instances) {
    ZNRecord zn = new ZNRecord(id);
    zn.setListField("0_0", instances);
    return zn;
  }

  @Test
  public void testGetInstancePartitionsOffline() {
    String rawTable = "myTable";
    String tableNameWithType = rawTable + "_OFFLINE";

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);

    InstancePartitions offline = new InstancePartitions(
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTable));
    offline.setInstances(0, 0, List.of("Server_0"));

    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt())).thenReturn(offline.toZNRecord());

    Map<String, InstancePartitions> result = _resource.getInstancePartitions(tableNameWithType, "OFFLINE", null);
    assertEquals(result.size(), 1);
    assertEquals(result.get("OFFLINE"), offline);
  }

  @Test
  public void testGetInstancePartitionsNotFoundThrows() {
    String rawTable = "notFound";
    String tableNameWithType = rawTable + "_OFFLINE";

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);

    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt())).thenReturn(null);
    assertThrows(ControllerApplicationException.class,
        () -> _resource.getInstancePartitions(tableNameWithType, "OFFLINE", null));
  }

  @Test
  public void testAssignInstancesDryRunNoPersistPreConfigured() {
    String rawTable = "t1";
    String tableNameWithType = rawTable + "_OFFLINE";

    // Table config with pre-configured instance partitions mapping
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(rawTable)
        .setInstancePartitionsMap(Map.of(InstancePartitionsType.OFFLINE, "preconfigured_name"))
        .build();

    when(_resourceManager.getOfflineTableConfig(tableNameWithType)).thenReturn(tableConfig);
    when(_resourceManager.getRealtimeTableConfig(tableNameWithType)).thenReturn(null);
    when(_resourceManager.getAllHelixInstanceConfigs()).thenReturn(Collections.emptyList());

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);

    InstancePartitions computed = new InstancePartitions(
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTable));
    computed.setInstances(0, 0, List.of("S0"));

    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt()))
        .thenReturn(createInstancePartitionsZN("preconfigured_name", List.of("S0")));

    Map<String, InstancePartitions> result = _resource.assignInstances(tableNameWithType, "OFFLINE", true, null);
    assertEquals(result.size(), 1);
    assertEquals(result.get("OFFLINE").getInstances(0, 0), List.of("S0"));
    verify(propertyStore, times(0)).set(anyString(), any(), anyInt());
  }

  @Test
  public void testAssignInstancesPersist() {
    String rawTable = "t2";
    String tableNameWithType = rawTable + "_OFFLINE";

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(rawTable)
        .setInstancePartitionsMap(Map.of(InstancePartitionsType.OFFLINE, "preconfigured_name"))
        .build();

    when(_resourceManager.getOfflineTableConfig(tableNameWithType)).thenReturn(tableConfig);
    when(_resourceManager.getRealtimeTableConfig(tableNameWithType)).thenReturn(null);
    when(_resourceManager.getAllHelixInstanceConfigs()).thenReturn(Collections.emptyList());

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);

    InstancePartitions computed = new InstancePartitions(
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTable));
    computed.setInstances(0, 0, List.of("S0"));

    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt()))
        .thenReturn(createInstancePartitionsZN("preconfigured_name", List.of("S0")));

    when(propertyStore.set(anyString(), Mockito.any(), anyInt())).thenReturn(true);
    ArgumentCaptor<ZNRecord> znCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    ArgumentCaptor<String> instancePartitionsNameCaptor = ArgumentCaptor.forClass(String.class);
    Map<String, InstancePartitions> result = _resource.assignInstances(tableNameWithType, "OFFLINE", false, null);
    assertEquals(result.size(), 1);
    assertEquals(result.get("OFFLINE").getInstances(0, 0), List.of("S0"));
    verify(propertyStore, times(1)).set(instancePartitionsNameCaptor.capture(), znCaptor.capture(), anyInt());
    assertEquals(InstancePartitions.fromZNRecord(znCaptor.getValue()).getInstances(0, 0), List.of("S0"));
    assertEquals(instancePartitionsNameCaptor.getValue(),
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(computed.getInstancePartitionsName()));
  }

  @Test
  public void testSetInstancePartitionsOfflinePersists() {
    String rawTable = "t3";
    String tableNameWithType = rawTable + "_OFFLINE";

    InstancePartitions ip = new InstancePartitions(
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTable));
    ip.setInstances(0, 0, List.of("S0", "S1"));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);
    when(propertyStore.set(anyString(), Mockito.any(), anyInt())).thenReturn(true);

    ArgumentCaptor<ZNRecord> znCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    ArgumentCaptor<String> instancePartitionsNameCaptor = ArgumentCaptor.forClass(String.class);
    Map<String, InstancePartitions> result =
        _resource.setInstancePartitions(tableNameWithType, ip.toJsonString(), null);
    assertEquals(result.size(), 1);
    assertEquals(result.get("OFFLINE"), ip);
    verify(propertyStore, times(1)).set(instancePartitionsNameCaptor.capture(), znCaptor.capture(), anyInt());
    assertEquals(InstancePartitions.fromZNRecord(znCaptor.getValue()), ip);
    assertEquals(instancePartitionsNameCaptor.getValue(),
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(ip.getInstancePartitionsName()));
  }

  @Test
  public void testSetInstancePartitionsBadNameRejected() {
    String rawTable = "t4";
    String tableNameWithType = rawTable + "_OFFLINE";

    InstancePartitions ip = new InstancePartitions("other_name");
    ip.setInstances(0, 0, List.of("S0"));

    assertThrows(ControllerApplicationException.class,
        () -> _resource.setInstancePartitions(tableNameWithType, ip.toJsonString(), null));
  }

  @Test
  public void testRemoveInstancePartitions() {
    String rawTable = "t5";
    String tableNameWithType = rawTable + "_REALTIME";

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);
    when(propertyStore.remove(anyString(), anyInt())).thenReturn(true);

    // Remove specific type
    _resource.removeInstancePartitions(tableNameWithType, "CONSUMING", null);
    ArgumentCaptor<String> instancePartitionsNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(propertyStore, times(1)).remove(instancePartitionsNameCaptor.capture(), anyInt());
    assertEquals(instancePartitionsNameCaptor.getValue(),
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
            InstancePartitionsUtils.getInstancePartitionsName(rawTable, InstancePartitionsType.CONSUMING.name())));

    // Remove all for realtime (2 more removes: CONSUMING + COMPLETED)
    instancePartitionsNameCaptor = ArgumentCaptor.forClass(String.class);
    _resource.removeInstancePartitions(tableNameWithType, null, null);
    verify(propertyStore, times(3)).remove(instancePartitionsNameCaptor.capture(), anyInt());
    assertEquals(instancePartitionsNameCaptor.getAllValues().get(1),
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
            InstancePartitionsUtils.getInstancePartitionsName(rawTable, InstancePartitionsType.CONSUMING.name())));
    assertEquals(instancePartitionsNameCaptor.getAllValues().get(2),
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
            InstancePartitionsUtils.getInstancePartitionsName(rawTable, InstancePartitionsType.COMPLETED.name())));
  }

  @Test
  public void testReplaceInstanceReplacesAndPersists() {
    String raw = "table";
    String tableNameWithType = raw + "_OFFLINE";

    InstancePartitions ip = new InstancePartitions(InstancePartitionsType.OFFLINE.getInstancePartitionsName(raw));
    ip.setInstances(0, 0, new ArrayList<>(List.of("old", "x")));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);
    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt())).thenReturn(ip.toZNRecord());
    when(propertyStore.set(anyString(), Mockito.any(), anyInt())).thenReturn(true);

    ArgumentCaptor<ZNRecord> znCaptor2 = ArgumentCaptor.forClass(ZNRecord.class);
    Map<String, InstancePartitions> result = _resource.replaceInstance(tableNameWithType, null, "old", "new", null);
    assertEquals(result.size(), 1);
    assertEquals(result.get("OFFLINE").getPartitionToInstancesMap().get("0_0"), List.of("new", "x"));
    verify(propertyStore, times(1)).set(anyString(), znCaptor2.capture(), anyInt());
    assertEquals(InstancePartitions.fromZNRecord(znCaptor2.getValue()).getInstances(0, 0), List.of("new", "x"));
  }

  @Test
  public void testReplaceInstanceNotFoundThrows() {
    String raw = "table";
    String tableNameWithType = raw + "_OFFLINE";

    InstancePartitions ip = new InstancePartitions(InstancePartitionsType.OFFLINE.getInstancePartitionsName(raw));
    ip.setInstances(0, 0, new ArrayList<>(List.of("a", "b")));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_resourceManager.getPropertyStore()).thenReturn(propertyStore);
    Mockito.when(propertyStore.get(anyString(), Mockito.isNull(), anyInt())).thenReturn(ip.toZNRecord());

    assertThrows(ControllerApplicationException.class,
        () -> _resource.replaceInstance(tableNameWithType, null, "missing", "new", null));
  }
}
