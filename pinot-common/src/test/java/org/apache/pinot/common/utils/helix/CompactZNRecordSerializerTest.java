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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Checks that [CompactZNRecordSerializer] returns records equal to those of [ZNRecordSerializer], with interned
/// names, shared instance-state maps and immutable map fields.
public class CompactZNRecordSerializerTest {
  private static final ZNRecordSerializer DEFAULT_SERIALIZER = new ZNRecordSerializer();
  private static final CompactZNRecordSerializer COMPACT_SERIALIZER = new CompactZNRecordSerializer();

  @DataProvider
  public Object[][] compression() {
    return new Object[][]{{false}, {true}};
  }

  private static byte[] bytes(ZNRecord record, boolean compressed)
      throws IOException {
    byte[] bytes = DEFAULT_SERIALIZER.serialize(record);
    if (compressed && !GZipCompressionUtil.isCompressed(bytes)) {
      bytes = GZipCompressionUtil.compress(bytes);
    }
    assertEquals(GZipCompressionUtil.isCompressed(bytes), compressed);
    return bytes;
  }

  private static ZNRecord assertSameAsDefault(byte[] bytes)
      throws IOException {
    ZNRecord expected = (ZNRecord) DEFAULT_SERIALIZER.deserialize(bytes);
    // Call the compact parser directly so that a parse failure cannot hide behind the fallback
    ZNRecord actual = CompactZNRecordSerializer.deserializeCompact(bytes);
    assertEquals(COMPACT_SERIALIZER.deserialize(bytes), actual);
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(actual.getId(), expected.getId());
    assertEquals(actual, expected);
    assertEquals(actual.getSimpleFields(), expected.getSimpleFields());
    assertEquals(actual.getListFields(), expected.getListFields());
    assertEquals(actual.getMapFields(), expected.getMapFields());
    assertEquals(actual.getRawPayload(), expected.getRawPayload());
    // Same iteration order for simple and list fields; map fields are sorted
    assertEquals(new ArrayList<>(actual.getSimpleFields().keySet()),
        new ArrayList<>(expected.getSimpleFields().keySet()));
    assertEquals(new ArrayList<>(actual.getListFields().keySet()), new ArrayList<>(expected.getListFields().keySet()));
    assertEquals(new ArrayList<>(actual.getMapFields().keySet()),
        new ArrayList<>(new TreeMap<>(expected.getMapFields()).keySet()));
    return actual;
  }

  private static ZNRecord sampleIdealState() {
    ZNRecord record = new ZNRecord("myTable_REALTIME");
    record.setSimpleField("IDEAL_STATE_MODE", "CUSTOMIZED");
    record.setSimpleField("REBALANCE_MODE", "CUSTOMIZED");
    record.setSimpleField("HELIX_ENABLED", "true");
    record.setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY, "1700000000000");
    record.setListField("emptyList", new ArrayList<>());
    record.setListField("myTable__0__1__20240101T0000Z", new ArrayList<>(List.of("Server_b_1", "Server_a_1")));
    for (int i = 0; i < 50; i++) {
      Map<String, String> stateMap = new LinkedHashMap<>();
      // Unsorted on purpose
      stateMap.put("Server_" + (i % 3 + 5) + "_8098", SegmentStateModel.ONLINE);
      stateMap.put("Server_" + (i % 3 + 1) + "_8098", i % 7 == 0 ? SegmentStateModel.CONSUMING : "ONLINE");
      record.setMapField("myTable__" + (49 - i) + "__0__20240101T0000Z", stateMap);
    }
    record.setMapField("myTable__unknown", Map.of("Server_1_8098", "SOME_NEW_STATE", "Server_2_8098", "ERROR"));
    record.setMapField("myTable__empty", Map.of());
    return record;
  }

  @Test(dataProvider = "compression")
  public void testRoundTripIdealState(boolean compressed)
      throws IOException {
    ZNRecord actual = assertSameAsDefault(bytes(sampleIdealState(), compressed));

    // IdealState and ExternalView wrappers work on the compact record
    IdealState idealState = new IdealState(actual);
    assertTrue(idealState.isEnabled());
    assertEquals(idealState.getRecord().getSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY),
        "1700000000000");
    assertEquals(idealState.getPartitionSet().size(), 52);
    assertEquals(idealState.getInstanceStateMap("myTable__unknown").get("Server_1_8098"), "SOME_NEW_STATE");
    ExternalView externalView = new ExternalView(actual);
    assertEquals(externalView.getStateMap("myTable__empty"), Map.of());
    assertTrue(externalView.getStateMap("myTable__0__0__20240101T0000Z") instanceof SortedMap);
  }

  @Test(dataProvider = "compression")
  public void testRoundTripEmptyAndMissingFields(boolean compressed)
      throws IOException {
    assertSameAsDefault(bytes(new ZNRecord("empty"), compressed));
    ZNRecord withPayload = new ZNRecord("payload");
    withPayload.setRawPayload(new byte[]{1, 2, 3, 4, 5});
    assertSameAsDefault(bytes(withPayload, compressed));

    // Hand-written JSON: missing fields, unknown properties, null and non-string scalar values, duplicate keys
    String json = "{\"id\":\"t_OFFLINE\",\"unknown\":{\"a\":[1,{\"b\":null}]},"
        + "\"simpleFields\":{\"n\":12,\"b\":true,\"nul\":null,\"dup\":\"1\",\"dup\":\"2\"},"
        + "\"mapFields\":{\"s2\":{\"z\":\"ONLINE\",\"a\":\"OFFLINE\",\"a\":\"ONLINE\"},\"s1\":{\"x\":null,\"y\":7},"
        + "\"s0\":null,\"s2\":{\"q\":\"DROPPED\"}},\"listFields\":{\"l\":[\"x\",null,\"y\"],\"nl\":null}}";
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    assertSameAsDefault(bytes);
    assertSameAsDefault(GZipCompressionUtil.compress(bytes));
    // Only the id
    assertSameAsDefault("{\"id\":\"only\"}".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testRoundTripRandomized()
      throws IOException {
    Random random = new Random(42);
    for (int iteration = 0; iteration < 20; iteration++) {
      ZNRecord record = new ZNRecord("table_" + iteration);
      int numSegments = random.nextInt(200);
      for (int i = 0; i < numSegments; i++) {
        Map<String, String> stateMap = new LinkedHashMap<>();
        int numReplicas = random.nextInt(5);
        for (int r = 0; r < numReplicas; r++) {
          String[] states = {"ONLINE", "OFFLINE", "CONSUMING", "ERROR", "OTHER"};
          stateMap.put("Server_" + random.nextInt(10), states[random.nextInt(states.length)]);
        }
        record.setMapField("segment_" + random.nextInt(1000), stateMap);
      }
      assertSameAsDefault(bytes(record, random.nextBoolean()));
    }
  }

  @Test
  public void testInterningAndSharing()
      throws IOException {
    ZNRecord actual = (ZNRecord) COMPACT_SERIALIZER.deserialize(bytes(sampleIdealState(), true));
    Map<String, Map<String, String>> mapFields = actual.getMapFields();
    assertTrue(mapFields instanceof ImmutableSortedArrayMap);
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      assertSame(entry.getKey(), entry.getKey().intern());
      Map<String, String> stateMap = entry.getValue();
      assertTrue(stateMap instanceof ImmutableSortedArrayMap);
      for (Map.Entry<String, String> stateEntry : stateMap.entrySet()) {
        assertSame(stateEntry.getKey(), stateEntry.getKey().intern());
        assertSame(stateEntry.getValue(), stateEntry.getValue().intern());
      }
    }
    // Segment 49 - i: i = 48 is ONLINE on Server_5, i = 42 is CONSUMING on Server_1
    assertSame(mapFields.get("myTable__1__0__20240101T0000Z").get("Server_5_8098"), SegmentStateModel.ONLINE);
    assertSame(mapFields.get("myTable__7__0__20240101T0000Z").get("Server_1_8098"), SegmentStateModel.CONSUMING);
    for (String instance : actual.getListField("myTable__0__1__20240101T0000Z")) {
      assertSame(instance, instance.intern());
    }

    // Equal instance-state maps share one instance; different ones do not
    // Segment 49 - i: i = 1 and i = 4 have the same servers and both ONLINE
    Map<String, String> stateMap48 = mapFields.get("myTable__48__0__20240101T0000Z");
    Map<String, String> stateMap45 = mapFields.get("myTable__45__0__20240101T0000Z");
    assertEquals(stateMap48, stateMap45);
    assertSame(stateMap48, stateMap45);
    // i = 0 is CONSUMING on Server_1, i = 3 is ONLINE on Server_1
    assertNotSame(mapFields.get("myTable__49__0__20240101T0000Z"), mapFields.get("myTable__46__0__20240101T0000Z"));

    // Map fields are immutable, scalar fields are not
    assertThrows(UnsupportedOperationException.class, () -> actual.setMapField("x", Map.of()));
    assertThrows(UnsupportedOperationException.class, () -> stateMap48.put("x", "ONLINE"));
    actual.setVersion(7);
    actual.setCreationTime(1L);
    actual.setModifiedTime(2L);
    actual.setEphemeralOwner(3L);
    actual.setSimpleField("new", "value");
    assertEquals(actual.getVersion(), 7);

    // HelixProperty copies the outer map into a TreeMap and keeps the inner maps
    IdealState idealState = new IdealState(actual);
    assertSame(idealState.getInstanceStateMap("myTable__48__0__20240101T0000Z"), stateMap48);
    assertEquals(idealState.getRecord().getMapFields(), mapFields);
  }

  @Test
  public void testSortInPlace() {
    String[] keys = {"c", "a", "b", "a", "d", null};
    Object[] values = {"1", "2", "3", "4", "5", null};
    int size = CompactZNRecordSerializer.sortInPlace(keys, values, 5);
    assertEquals(size, 4);
    assertEquals(Arrays.copyOf(keys, size), new String[]{"a", "b", "c", "d"});
    assertEquals(Arrays.copyOf(values, size), new Object[]{"4", "3", "1", "5"});
  }

  @Test
  public void testMalformedInputFallsBack()
      throws IOException {
    // Both deserializers return null for malformed JSON
    byte[] malformed = "{\"id\":\"x\",\"mapFields\":{\"s\":[1]}}".getBytes(StandardCharsets.UTF_8);
    assertNull(DEFAULT_SERIALIZER.deserialize(malformed));
    assertNull(COMPACT_SERIALIZER.deserialize(malformed));
    // Gzip-compressed: the compact parser fails and falls back to the default deserializer
    byte[] compressedMalformed = GZipCompressionUtil.compress(malformed);
    assertThrows(IOException.class, () -> CompactZNRecordSerializer.deserializeCompact(compressedMalformed));
    assertNull(DEFAULT_SERIALIZER.deserialize(compressedMalformed));
    assertNull(COMPACT_SERIALIZER.deserialize(compressedMalformed));
    assertNull(COMPACT_SERIALIZER.deserialize("not json".getBytes(StandardCharsets.UTF_8)));
    assertNull(COMPACT_SERIALIZER.deserialize(new byte[0]));
    assertNull(COMPACT_SERIALIZER.deserialize(null));
  }

  @Test(dataProvider = "compression")
  public void testOnlyGzipUsesCompactParser(boolean compressed)
      throws IOException {
    byte[] bytes = bytes(sampleIdealState(), compressed);
    ZNRecord expected = (ZNRecord) DEFAULT_SERIALIZER.deserialize(bytes);
    ZNRecord actual = (ZNRecord) COMPACT_SERIALIZER.deserialize(bytes);
    assertNotNull(actual);
    assertEquals(actual, expected);
    assertEquals(actual.getMapFields(), expected.getMapFields());
    assertEquals(HelixHelper.isCompact(actual), compressed);
    Map<String, String> stateMap = actual.getMapField("myTable__0__0__20240101T0000Z");
    if (compressed) {
      assertTrue(actual.getMapFields() instanceof ImmutableSortedArrayMap);
      assertTrue(stateMap instanceof ImmutableSortedArrayMap);
    } else {
      // A plain znode goes to the default deserializer: same map types, mutable maps
      assertEquals(actual.getMapFields().getClass(), expected.getMapFields().getClass());
      assertEquals(stateMap.getClass(), expected.getMapField("myTable__0__0__20240101T0000Z").getClass());
      assertFalse(stateMap instanceof ImmutableSortedArrayMap);
      stateMap.put("Server_new_8098", SegmentStateModel.ONLINE);
      actual.setMapField("newSegment", new TreeMap<>());
      assertEquals(actual.getMapFields().size(), expected.getMapFields().size() + 1);
    }
  }

  @Test
  public void testSerializeDelegates()
      throws IOException {
    ZNRecord record = sampleIdealState();
    byte[] expected = DEFAULT_SERIALIZER.serialize(record);
    assertEquals(COMPACT_SERIALIZER.serialize(record), expected);
    // A compact record serializes to the same bytes as an equal record from the default deserializer
    ZNRecord compact = CompactZNRecordSerializer.deserializeCompact(expected);
    ZNRecord standard = (ZNRecord) DEFAULT_SERIALIZER.deserialize(expected);
    assertTrue(HelixHelper.isCompact(compact));
    assertEquals(DEFAULT_SERIALIZER.deserialize(COMPACT_SERIALIZER.serialize(compact)), standard);
  }

  @Test(dataProvider = "compression")
  public void testToIdealStateAndExternalViewShareCompactRecord(boolean compressed)
      throws IOException {
    ZNRecord written = sampleIdealState();
    written.setSimpleField("HELIX_ENABLED", "false");
    written.setRawPayload(new byte[]{1, 2, 3});
    byte[] bytes = bytes(written, compressed);

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
    byte[] bytes = DEFAULT_SERIALIZER.serialize(sampleIdealState());
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
}
