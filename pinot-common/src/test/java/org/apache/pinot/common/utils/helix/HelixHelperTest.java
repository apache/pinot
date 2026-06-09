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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class HelixHelperTest {
  private static final ZNRecordSerializer DEFAULT_SERIALIZER = new ZNRecordSerializer();

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
  public void testToIdealStateAndExternalViewShareCompactRecord()
      throws IOException {
    ZNRecord written = CompactZNRecordSerializerTest.sampleIdealState();
    written.setSimpleField("HELIX_ENABLED", "false");
    written.setRawPayload(new byte[]{1, 2, 3});
    // The compact parser only takes gzip-compressed content
    byte[] bytes = GZipCompressionUtil.compress(DEFAULT_SERIALIZER.serialize(written));

    for (boolean externalView : new boolean[]{false, true}) {
      ZNRecord compact = CompactZNRecordSerializer.deserializeCompact(bytes);
      setScalars(compact);
      assertTrue(HelixHelper.isCompact(compact));
      ZNRecord copied = (ZNRecord) DEFAULT_SERIALIZER.deserialize(bytes);
      setScalars(copied);
      assertFalse(HelixHelper.isCompact(copied));
      HelixProperty expected = externalView ? new ExternalView(copied) : new IdealState(copied);

      HelixProperty actual = externalView ? HelixHelper.toExternalView(compact)
          : HelixHelper.toIdealState(compact);
      assertTrue(externalView ? actual instanceof ExternalView : actual instanceof IdealState);
      ZNRecord record = actual.getRecord();
      // No copy: the maps and the payload are the ones of the parsed record
      assertSame(record.getMapFields(), compact.getMapFields());
      assertSame(record.getSimpleFields(), compact.getSimpleFields());
      assertSame(record.getListFields(), compact.getListFields());
      assertSame(record.getRawPayload(), compact.getRawPayload());
      // Same content, scalar fields and stat as the copying constructor
      assertEquals(actual.getId(), expected.getId());
      assertEquals(record, expected.getRecord());
      assertEquals(record.getSimpleFields(), expected.getRecord().getSimpleFields());
      assertEquals(record.getListFields(), expected.getRecord().getListFields());
      assertEquals(record.getMapFields(), expected.getRecord().getMapFields());
      assertEquals(record.getRawPayload(), expected.getRecord().getRawPayload());
      assertEquals(record.getVersion(), 17);
      assertEquals(record.getCreationTime(), 1000L);
      assertEquals(record.getModifiedTime(), 2000L);
      assertEquals(record.getEphemeralOwner(), 3000L);
      assertEquals(actual.getStat(), expected.getStat());
      assertEquals(actual.getStat().getVersion(), 17);
      assertEquals(actual.getBucketSize(), expected.getBucketSize());
      if (externalView) {
        assertEquals(((ExternalView) actual).getPartitionSet(), ((ExternalView) expected).getPartitionSet());
        assertSame(((ExternalView) actual).getStateMap("myTable__0__0__20240101T0000Z"),
            compact.getMapField("myTable__0__0__20240101T0000Z"));
      } else {
        IdealState idealState = (IdealState) actual;
        assertFalse(idealState.isEnabled());
        assertEquals(idealState.isEnabled(), ((IdealState) expected).isEnabled());
        assertEquals(idealState.getRebalanceMode(), IdealState.RebalanceMode.CUSTOMIZED);
        assertEquals(idealState.getPartitionSet(), ((IdealState) expected).getPartitionSet());
        assertEquals(idealState.getRecord().getSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY),
            "1700000000000");
      }
      // The shared outer map stays immutable
      assertThrows(UnsupportedOperationException.class, () -> record.getMapFields().remove("myTable__empty"));
    }
  }

  @Test
  public void testToIdealStateAndExternalViewCopyDefaultRecord() {
    byte[] bytes = DEFAULT_SERIALIZER.serialize(CompactZNRecordSerializerTest.sampleIdealState());
    for (boolean externalView : new boolean[]{false, true}) {
      // A record from the default deserializer (or from the fallback) goes through the copying constructor
      ZNRecord standard = (ZNRecord) DEFAULT_SERIALIZER.deserialize(bytes);
      assertNotNull(standard);
      setScalars(standard);
      assertFalse(HelixHelper.isCompact(standard));
      HelixProperty actual = externalView ? HelixHelper.toExternalView(standard)
          : HelixHelper.toIdealState(standard);
      HelixProperty expected = externalView ? new ExternalView(standard) : new IdealState(standard);
      assertNotSame(actual.getRecord(), standard);
      assertNotSame(actual.getRecord().getMapFields(), standard.getMapFields());
      assertTrue(actual.getRecord().getMapFields() instanceof TreeMap);
      assertEquals(actual.getRecord(), expected.getRecord());
      assertEquals(actual.getStat(), expected.getStat());
      // The copy is mutable, as today
      actual.getRecord().getMapFields().remove("myTable__empty");
      assertTrue(standard.getMapFields().containsKey("myTable__empty"));
    }
  }

  private static void setScalars(ZNRecord record) {
    record.setVersion(17);
    record.setCreationTime(1000L);
    record.setModifiedTime(2000L);
    record.setEphemeralOwner(3000L);
  }

  @Test
  public void testCloneIdealState() {
    IdealState idealState = new IdealState("myTable_REALTIME");
    ZNRecord record = idealState.getRecord();
    record.setSimpleField("REPLICAS", "2");
    record.setBooleanField("enableCompression", true);
    // Insertion order that differs from the sorted order, as in a deserialized ZNRecord
    Map<String, Map<String, String>> mapFields = new LinkedHashMap<>();
    Map<String, String> instanceStateMap = new LinkedHashMap<>();
    instanceStateMap.put("Server_2", "CONSUMING");
    instanceStateMap.put("Server_1", "CONSUMING");
    mapFields.put("segment_2", instanceStateMap);
    mapFields.put("segment_1", new LinkedHashMap<>(Map.of("Server_1", "ONLINE")));
    record.setMapFields(mapFields);
    record.setListField("segment_2", new ArrayList<>(List.of("Server_2", "Server_1")));
    record.setRawPayload(new byte[]{1, 2, 3});
    record.setVersion(5);

    // Expected result of cloning through a serialization round trip
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord expected =
        new IdealState((ZNRecord) serializer.deserialize(serializer.serialize(record))).getRecord();
    ZNRecord copy = HelixHelper.cloneIdealState(idealState).getRecord();

    // Same content and iteration order
    assertEquals(copy.getId(), expected.getId());
    assertEquals(copy, expected);
    assertEquals(new ArrayList<>(copy.getMapFields().keySet()), new ArrayList<>(expected.getMapFields().keySet()));
    assertEquals(new ArrayList<>(copy.getMapField("segment_2").keySet()),
        new ArrayList<>(expected.getMapField("segment_2").keySet()));
    assertEquals(copy.getRawPayload(), expected.getRawPayload());
    assertEquals(copy.getVersion(), expected.getVersion());

    // Modifying the copy does not modify the original
    assertNotSame(copy.getRawPayload(), record.getRawPayload());
    copy.getSimpleFields().put("REPLICAS", "3");
    copy.getMapField("segment_2").put("Server_2", "ONLINE");
    copy.getMapFields().remove("segment_1");
    copy.getListField("segment_2").add("Server_3");
    assertEquals(record.getSimpleField("REPLICAS"), "2");
    assertEquals(record.getMapField("segment_2").get("Server_2"), "CONSUMING");
    assertTrue(record.getMapFields().containsKey("segment_1"));
    assertEquals(record.getListField("segment_2"), List.of("Server_2", "Server_1"));
  }

  /// cloneIdealState copies a ZNRecord field by field, so it must know every field an IdealState stores in ZK. If a
  /// Helix upgrade fails this test, make cloneIdealState copy the new state (or leave it out if it is not stored, like
  /// the ZK stat fields), then update the expected values.
  @Test
  public void testCloneIdealStateKnowsAllStoredFields()
      throws IOException {
    // Instance fields, including inherited ones, sorted
    assertEquals(getInstanceFieldNames(ZNRecord.class),
        List.of("_creationTime", "_deltaList", "_ephemeralOwner", "_modifiedTime", "_serializer", "_version", "id",
            "listFields", "mapFields", "rawPayload", "simpleFields"));
    assertEquals(getInstanceFieldNames(IdealState.class), List.of("_record", "_stat"));

    // Of these, only the following are stored in ZK: the properties the serializer's mapper can write, whatever their
    // values, and the properties it writes for a record with all of them set
    Set<String> storedFields = Set.of("id", "simpleFields", "mapFields", "listFields", "rawPayload");
    ObjectMapper mapper = ZNRecordSerializerMapper.get();
    assertEquals(mapper.getSerializationConfig().introspect(mapper.constructType(ZNRecord.class)).findProperties()
        .stream()
        .filter(BeanPropertyDefinition::couldSerialize)
        .map(BeanPropertyDefinition::getName)
        .collect(Collectors.toSet()), storedFields);
    ZNRecord record = new ZNRecord("myTable_REALTIME");
    record.setSimpleField("REPLICAS", "2");
    record.setMapField("segment_1", Map.of("Server_1", "ONLINE"));
    record.setListField("segment_1", List.of("Server_1"));
    record.setRawPayload(new byte[]{1});
    byte[] bytes = new ZNRecordSerializer().serialize(record);
    if (GZipCompressionUtil.isCompressed(bytes)) {
      bytes = GZipCompressionUtil.uncompress(new ByteArrayInputStream(bytes));
    }
    Set<String> writtenFields = new HashSet<>();
    JsonUtils.bytesToJsonNode(bytes).fieldNames().forEachRemaining(writtenFields::add);
    assertEquals(writtenFields, storedFields);
  }

  /// Keeps duplicates, so that a field hiding an inherited one is caught.
  private static List<String> getInstanceFieldNames(Class<?> clazz) {
    List<String> fieldNames = new ArrayList<>();
    for (Class<?> c = clazz; c != Object.class; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
          fieldNames.add(field.getName());
        }
      }
    }
    fieldNames.sort(null);
    return fieldNames;
  }

  /// Exposes the ObjectMapper that ZNRecordSerializer writes ZNRecords with.
  private static class ZNRecordSerializerMapper extends ZNRecordSerializer {
    static ObjectMapper get() {
      return mapper;
    }
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
