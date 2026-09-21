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
package org.apache.pinot.query.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests the two wire encodings of the leaf-stage segment maps in [QueryPlanSerDeUtils]: the native proto fields and
/// the legacy JSON custom properties, including a server decoding what a pre-proto broker sends.
public class QueryPlanSerDeUtilsTest {
  private static final Map<String, String> CUSTOM_PROPERTIES = Map.of("foo", "bar");
  private static final Map<String, List<String>> TABLE_SEGMENTS_MAP =
      Map.of("OFFLINE", List.of("seg_0", "seg_1"), "REALTIME", List.of("seg__0__0__20240101T0000Z"));
  private static final Map<String, List<String>> LOGICAL_TABLE_SEGMENTS_MAP =
      Map.of("t1_OFFLINE", List.of("t1_seg_0"), "t2_REALTIME", List.of("t2_seg_0", "t2_seg_1"));

  @DataProvider
  public static Object[][] encodings() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "encodings")
  public void testLeafWorkerRoundTrip(boolean protoSegmentList)
      throws Exception {
    WorkerMetadata workerMetadata = leafWorker(TABLE_SEGMENTS_MAP, null);

    Worker.WorkerMetadata proto = toProto(workerMetadata, protoSegmentList);
    assertEquals(proto.hasTableSegmentsMap(), protoSegmentList);
    assertFalse(proto.hasLogicalTableSegmentsMap());
    assertEquals(proto.getCustomPropertyMap().containsKey(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY), !protoSegmentList);
    assertFalse(proto.getCustomPropertyMap().containsKey(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY));
    assertEquals(proto.getCustomPropertyMap().get("foo"), "bar");

    WorkerMetadata decoded = QueryPlanSerDeUtils.fromProtoWorkerMetadata(proto);
    assertEquals(decoded.getWorkerId(), 3);
    assertEquals(decoded.getTableSegmentsMap(), TABLE_SEGMENTS_MAP);
    assertNull(decoded.getLogicalTableSegmentsMap());
    assertTrue(decoded.isLeafStageWorker());
    // The JSON is never surfaced as a custom property of the decoded metadata, whichever encoding was used.
    assertEquals(decoded.getCustomProperties(), CUSTOM_PROPERTIES);
    MailboxInfo mailboxInfo = decoded.getMailboxInfosMap().get(2).getMailboxInfos().get(0);
    assertEquals(mailboxInfo.getHostname(), "localhost");
    assertEquals(mailboxInfo.getPort(), 1234);
    assertEquals(mailboxInfo.getWorkerIds(), List.of(0, 1));
  }

  @Test(dataProvider = "encodings")
  public void testLogicalTableLeafWorkerRoundTrip(boolean protoSegmentList)
      throws Exception {
    WorkerMetadata workerMetadata = leafWorker(null, LOGICAL_TABLE_SEGMENTS_MAP);

    Worker.WorkerMetadata proto = toProto(workerMetadata, protoSegmentList);
    assertFalse(proto.hasTableSegmentsMap());
    assertEquals(proto.hasLogicalTableSegmentsMap(), protoSegmentList);
    assertEquals(proto.getCustomPropertyMap().containsKey(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY),
        !protoSegmentList);

    WorkerMetadata decoded = QueryPlanSerDeUtils.fromProtoWorkerMetadata(proto);
    assertNull(decoded.getTableSegmentsMap());
    assertEquals(decoded.getLogicalTableSegmentsMap(), LOGICAL_TABLE_SEGMENTS_MAP);
    assertTrue(decoded.isLeafStageWorker());
    assertEquals(decoded.getCustomProperties(), CUSTOM_PROPERTIES);
  }

  @Test(dataProvider = "encodings")
  public void testEmptySegmentListStillMarksLeafWorker(boolean protoSegmentList)
      throws Exception {
    // A padded worker of a partitioned table scans no segment but must still run the leaf stage.
    Map<String, List<String>> emptySegments = Map.of("OFFLINE", new ArrayList<>());
    WorkerMetadata decoded =
        QueryPlanSerDeUtils.fromProtoWorkerMetadata(toProto(leafWorker(emptySegments, null), protoSegmentList));
    assertEquals(decoded.getTableSegmentsMap(), emptySegments);
    assertTrue(decoded.isLeafStageWorker());
  }

  /// A segments map with no entries at all encodes, in the proto encoding, to `SegmentsMap.getDefaultInstance()`.
  /// The leaf/intermediate distinction rides on proto3's explicit presence for singular message fields, which must
  /// keep that default instance on the wire rather than drop the field, so this goes through real bytes.
  @Test(dataProvider = "encodings")
  public void testZeroEntrySegmentsMapStillMarksLeafWorker(boolean protoSegmentList)
      throws Exception {
    Worker.WorkerMetadata proto = toProto(leafWorker(Map.of(), Map.of()), protoSegmentList);
    assertEquals(proto.hasTableSegmentsMap(), protoSegmentList);
    assertEquals(proto.hasLogicalTableSegmentsMap(), protoSegmentList);

    WorkerMetadata decoded = QueryPlanSerDeUtils.fromProtoWorkerMetadata(proto);
    assertEquals(decoded.getTableSegmentsMap(), Map.of());
    assertEquals(decoded.getLogicalTableSegmentsMap(), Map.of());
    assertTrue(decoded.isLeafStageWorker());
  }

  /// Whether a server path may write to the decoded custom properties must not depend on the encoding the broker
  /// picked: the legacy decode strips the JSON keys from a copy, which has to stay as unmodifiable as the proto view.
  @Test(dataProvider = "encodings")
  public void testDecodedCustomPropertiesAreUnmodifiable(boolean protoSegmentList)
      throws Exception {
    WorkerMetadata leaf =
        QueryPlanSerDeUtils.fromProtoWorkerMetadata(toProto(leafWorker(TABLE_SEGMENTS_MAP, null), protoSegmentList));
    assertThrows(UnsupportedOperationException.class, () -> leaf.getCustomProperties().put("k", "v"));

    WorkerMetadata intermediate = QueryPlanSerDeUtils.fromProtoWorkerMetadata(
        toProto(new WorkerMetadata(1, Map.of(), new HashMap<>(CUSTOM_PROPERTIES)), protoSegmentList));
    assertThrows(UnsupportedOperationException.class, () -> intermediate.getCustomProperties().put("k", "v"));
  }

  @Test(dataProvider = "encodings")
  public void testIntermediateWorkerRoundTrip(boolean protoSegmentList)
      throws Exception {
    WorkerMetadata workerMetadata = new WorkerMetadata(1, Map.of(), new HashMap<>(CUSTOM_PROPERTIES));

    Worker.WorkerMetadata proto = toProto(workerMetadata, protoSegmentList);
    assertFalse(proto.hasTableSegmentsMap());
    assertFalse(proto.hasLogicalTableSegmentsMap());
    assertEquals(proto.getCustomPropertyMap(), CUSTOM_PROPERTIES);

    WorkerMetadata decoded = QueryPlanSerDeUtils.fromProtoWorkerMetadata(proto);
    assertNull(decoded.getTableSegmentsMap());
    assertNull(decoded.getLogicalTableSegmentsMap());
    assertFalse(decoded.isLeafStageWorker());
    assertEquals(decoded.getCustomProperties(), CUSTOM_PROPERTIES);
  }

  /// A broker that predates the proto fields ships Jackson-encoded JSON custom properties; a new server must decode
  /// exactly that.
  @Test
  public void testDecodesLegacyBrokerJsonCustomProperties()
      throws Exception {
    Worker.WorkerMetadata proto = Worker.WorkerMetadata.newBuilder().setWorkedId(7)
        .putCustomProperty(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY, JsonUtils.objectToString(TABLE_SEGMENTS_MAP))
        .putCustomProperty(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY,
            JsonUtils.objectToString(LOGICAL_TABLE_SEGMENTS_MAP))
        .putCustomProperty("foo", "bar")
        .build();

    WorkerMetadata decoded = QueryPlanSerDeUtils.fromProtoWorkerMetadata(proto);
    assertEquals(decoded.getWorkerId(), 7);
    assertEquals(decoded.getTableSegmentsMap(), TABLE_SEGMENTS_MAP);
    assertEquals(decoded.getLogicalTableSegmentsMap(), LOGICAL_TABLE_SEGMENTS_MAP);
    assertTrue(decoded.isLeafStageWorker());
    assertEquals(decoded.getCustomProperties(), CUSTOM_PROPERTIES);
  }

  /// The legacy encoding must stay readable by a server that predates the proto fields, which parses the custom
  /// property with Jackson: pin the exact JSON shape it expects.
  @Test
  public void testLegacyEncodingIsTheJacksonJsonOlderServersParse()
      throws Exception {
    Map<String, List<String>> segmentsMap = Map.of("OFFLINE", List.of("seg_0", "seg-1.tar.gz", "s\u00ebg_2"));
    Worker.WorkerMetadata proto = toProto(leafWorker(segmentsMap, null), false);
    assertEquals(proto.getCustomPropertyMap().get(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY),
        "{\"OFFLINE\":[\"seg_0\",\"seg-1.tar.gz\",\"s\u00ebg_2\"]}");
  }

  private static WorkerMetadata leafWorker(@Nullable Map<String, List<String>> tableSegmentsMap,
      @Nullable Map<String, List<String>> logicalTableSegmentsMap) {
    MailboxInfos mailboxInfos = new MailboxInfos(new MailboxInfo("localhost", 1234, List.of(0, 1)));
    WorkerMetadata workerMetadata = new WorkerMetadata(3, Map.of(2, mailboxInfos), new HashMap<>(CUSTOM_PROPERTIES));
    if (tableSegmentsMap != null) {
      workerMetadata.setTableSegmentsMap(tableSegmentsMap);
    }
    if (logicalTableSegmentsMap != null) {
      workerMetadata.setLogicalTableSegmentsMap(logicalTableSegmentsMap);
    }
    return workerMetadata;
  }

  /// Serializes through the public list API and parses the bytes back, as the server does.
  private static Worker.WorkerMetadata toProto(WorkerMetadata workerMetadata, boolean protoSegmentList)
      throws Exception {
    Worker.WorkerMetadata proto =
        QueryPlanSerDeUtils.toProtoWorkerMetadataList(List.of(workerMetadata), protoSegmentList).get(0);
    return Worker.WorkerMetadata.parseFrom(proto.toByteString());
  }
}
