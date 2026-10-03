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
import java.util.stream.Collectors;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
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
}
