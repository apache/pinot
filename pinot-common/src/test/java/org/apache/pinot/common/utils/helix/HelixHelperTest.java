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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

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
}
