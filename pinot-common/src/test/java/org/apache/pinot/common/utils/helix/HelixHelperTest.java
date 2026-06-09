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
package org.apache.pinot.common.utils.helix;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class HelixHelperTest {

  @Test
  public void testUpdateHostName() {
    String instanceId = "Server_myInstance";
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertNull(instanceConfig.getHostName());
    assertNull(instanceConfig.getPort());

    assertTrue(HelixHelper.updateHostnamePort(instanceConfig, "myHost", 1234));
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");

    assertTrue(HelixHelper.updateHostnamePort(instanceConfig, "myHost2", 1234));
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost2");
    assertEquals(instanceConfig.getPort(), "1234");

    assertTrue(HelixHelper.updateHostnamePort(instanceConfig, "myHost2", 2345));
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost2");
    assertEquals(instanceConfig.getPort(), "2345");

    assertFalse(HelixHelper.updateHostnamePort(instanceConfig, "myHost2", 2345));
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost2");
    assertEquals(instanceConfig.getPort(), "2345");
  }

  @Test
  public void testAddDefaultTags() {
    String instanceId = "Server_myInstance";
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    List<String> defaultTags = Arrays.asList("tag1", "tag2");
    assertTrue(HelixHelper.addDefaultTags(instanceConfig, () -> defaultTags));
    assertEquals(instanceConfig.getTags(), defaultTags);

    assertFalse(HelixHelper.addDefaultTags(instanceConfig, () -> defaultTags));
    assertEquals(instanceConfig.getTags(), defaultTags);

    List<String> otherTags = Arrays.asList("tag3", "tag4");
    assertFalse(HelixHelper.addDefaultTags(instanceConfig, () -> otherTags));
    assertEquals(instanceConfig.getTags(), defaultTags);
  }

  @Test
  public void testRemoveDisabledPartitions() {
    String instanceId = "Server_myInstance";
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    assertTrue(instanceConfig.getDisabledPartitionsMap().isEmpty());
    assertFalse(HelixHelper.removeDisabledPartitions(instanceConfig));

    instanceConfig.setInstanceEnabledForPartition("myResource", "myPartition", false);
    assertFalse(instanceConfig.getDisabledPartitionsMap().isEmpty());
    assertTrue(HelixHelper.removeDisabledPartitions(instanceConfig));
    assertTrue(instanceConfig.getDisabledPartitionsMap().isEmpty());
  }

  @Test
  public void testAtomicInstanceConfigUpdateMutatesLatestRecord() {
    String instanceId = "Broker_myInstance";
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey propertyKey = mock(PropertyKey.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.instanceConfig(instanceId)).thenReturn(propertyKey);
    InstanceConfig latestConfig = new InstanceConfig(instanceId);
    latestConfig.getRecord().setBooleanField("concurrentMarker", true);
    AtomicReference<ZNRecord> persistedRecord = new AtomicReference<>();
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      persistedRecord.set(updater.update(new ZNRecord(latestConfig.getRecord())));
      return true;
    }).when(helixDataAccessor).updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class));

    assertTrue(HelixHelper.updateInstanceConfig(helixDataAccessor, instanceId,
        instanceConfig -> instanceConfig.addTag("tenant_BROKER")));

    InstanceConfig persistedConfig = new InstanceConfig(persistedRecord.get());
    assertTrue(persistedConfig.getRecord().getBooleanField("concurrentMarker", false));
    assertEquals(persistedConfig.getTags(), List.of("tenant_BROKER"));
  }

  @Test
  public void testAtomicInstanceConfigUpdateReturnsFalseForMissingInstance() {
    String instanceId = "Broker_missing";
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey propertyKey = mock(PropertyKey.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.instanceConfig(instanceId)).thenReturn(propertyKey);
    AtomicBoolean mutatorInvoked = new AtomicBoolean();
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      assertNull(updater.update(null));
      return true;
    }).when(helixDataAccessor).updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class));

    assertFalse(HelixHelper.updateInstanceConfig(helixDataAccessor, instanceId,
        instanceConfig -> mutatorInvoked.set(true)));
    assertFalse(mutatorInvoked.get());
  }

  @Test
  public void testUpdateBrokerResourceRefreshesBrokerTagsOnIdealStateRetry() {
    String clusterName = "testCluster";
    String brokerId = "Broker_localhost_8099";
    String oldTenant = "oldTenant";
    String newTenant = "newTenant";
    String oldBrokerTag = oldTenant + "_BROKER";
    String newBrokerTag = newTenant + "_BROKER";

    TableConfig oldTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("oldTable")
        .setBrokerTenant(oldTenant).build();
    TableConfig newTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("newTable")
        .setBrokerTenant(newTenant).build();
    String oldTable = oldTableConfig.getTableName();
    String newTable = newTableConfig.getTableName();

    InstanceConfig oldInstanceConfig = new InstanceConfig(brokerId);
    oldInstanceConfig.addTag(oldBrokerTag);
    InstanceConfig newInstanceConfig = new InstanceConfig(brokerId);
    newInstanceConfig.addTag(newBrokerTag);

    IdealState brokerResource = new IdealState(HelixHelper.BROKER_RESOURCE);
    brokerResource.setPartitionState(oldTable, "Broker_other_8099", "ONLINE");
    brokerResource.setPartitionState(newTable, "Broker_other_8099", "ONLINE");

    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey idealStateKey = mock(PropertyKey.class);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getClusterName()).thenReturn(clusterName);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    when(helixAdmin.getInstanceConfig(clusterName, brokerId)).thenReturn(oldInstanceConfig, newInstanceConfig);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.idealStates(HelixHelper.BROKER_RESOURCE)).thenReturn(idealStateKey);
    when(idealStateKey.getPath()).thenReturn("/test/IDEALSTATES/BrokerResource");
    when(helixDataAccessor.getProperty(idealStateKey)).thenReturn(brokerResource);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);

    AtomicInteger writeAttempts = new AtomicInteger();
    AtomicReference<ZNRecord> persistedRecord = new AtomicReference<>();
    when(baseDataAccessor.set(eq("/test/IDEALSTATES/BrokerResource"), any(ZNRecord.class), anyInt(),
        eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
          if (writeAttempts.getAndIncrement() == 0) {
            return false;
          }
          persistedRecord.set(new ZNRecord(invocation.getArgument(1, ZNRecord.class)));
          return true;
        });

    try (MockedStatic<ZKMetadataProvider> metadataProvider = org.mockito.Mockito.mockStatic(ZKMetadataProvider.class)) {
      metadataProvider.when(() -> ZKMetadataProvider.getAllTableConfigs(propertyStore))
          .thenReturn(List.of(oldTableConfig, newTableConfig));
      metadataProvider.when(() -> ZKMetadataProvider.getAllLogicalTableConfigs(propertyStore)).thenReturn(List.of());

      // The caller captured the old tag, but the live InstanceConfig changes before the IdealState retry.
      HelixHelper.updateBrokerResource(helixManager, brokerId, List.of(oldBrokerTag), null, null);
    }

    IdealState persistedIdealState = new IdealState(persistedRecord.get());
    assertFalse(persistedIdealState.getInstanceSet(oldTable).contains(brokerId));
    assertTrue(persistedIdealState.getInstanceSet(newTable).contains(brokerId));
    assertEquals(writeAttempts.get(), 2);
    verify(helixAdmin, times(2)).getInstanceConfig(clusterName, brokerId);
  }
}
