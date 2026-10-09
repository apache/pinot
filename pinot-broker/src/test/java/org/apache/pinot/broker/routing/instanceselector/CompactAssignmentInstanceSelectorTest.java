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
package org.apache.pinot.broker.routing.instanceselector;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.helix.CompactZNRecordSerializer;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.ImmutableSortedArrayMap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Checks that the instance selectors route the same way, and do not mutate the assignment, when the ideal state and
/// external view come from [CompactZNRecordSerializer] instead of [ZNRecordSerializer].
public class CompactAssignmentInstanceSelectorTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final InstanceSelectorConfig CONFIG = new InstanceSelectorConfig(false, 300, false);

  @DataProvider
  public Object[][] selectorTypes() {
    return new Object[][]{{"balanced", false}, {"replicaGroup", false}, {"strictReplicaGroup", true}};
  }

  private static InstanceSelector newSelector(String type) {
    switch (type) {
      case "balanced":
        return new BalancedInstanceSelector();
      case "replicaGroup":
        return new ReplicaGroupInstanceSelector();
      default:
        return new StrictReplicaGroupInstanceSelector();
    }
  }

  @Test(dataProvider = "selectorTypes")
  public void testSameSelection(String type, boolean upsert) {
    Random random = new Random(7);
    List<String> instances = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      instances.add("Server_host" + i + "_8098");
    }
    ZNRecord idealStateRecord = new ZNRecord(TABLE_NAME);
    idealStateRecord.setSimpleField("REBALANCE_MODE", "CUSTOMIZED");
    ZNRecord externalViewRecord = new ZNRecord(TABLE_NAME);
    // Compressed, because the compact serializer only parses gzip-compressed znodes itself
    idealStateRecord.setBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, true);
    externalViewRecord.setBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, true);
    List<String> segments = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      String segment = "segment_" + i;
      segments.add(segment);
      // Replica groups: (0, 3), (1, 4), (2, 5); insertion order is not sorted on purpose
      int group = i % 3;
      Map<String, String> idealStateMap = new LinkedHashMap<>();
      idealStateMap.put(instances.get(group + 3), i % 10 == 0 ? CONSUMING : ONLINE);
      idealStateMap.put(instances.get(group), ONLINE);
      idealStateRecord.setMapField(segment, idealStateMap);
      if (i % 50 == 1) {
        // Missing from the external view
        continue;
      }
      Map<String, String> externalViewMap = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : idealStateMap.entrySet()) {
        int r = random.nextInt(20);
        externalViewMap.put(entry.getKey(), r == 0 ? ERROR : r == 1 ? OFFLINE : entry.getValue());
      }
      externalViewRecord.setMapField(segment, externalViewMap);
    }
    ZNRecordSerializer defaultSerializer = new ZNRecordSerializer();
    byte[] idealStateBytes = defaultSerializer.serialize(idealStateRecord);
    byte[] externalViewBytes = defaultSerializer.serialize(externalViewRecord);
    assertTrue(GZipCompressionUtil.isCompressed(idealStateBytes));
    assertTrue(GZipCompressionUtil.isCompressed(externalViewBytes));
    Set<String> enabledInstances = new HashSet<>(instances);

    List<Map<String, String>> expected =
        select(type, upsert, defaultSerializer, idealStateBytes, externalViewBytes, enabledInstances, segments, false);
    List<Map<String, String>> actual =
        select(type, upsert, new CompactZNRecordSerializer(), idealStateBytes, externalViewBytes, enabledInstances,
            segments, true);
    assertEquals(actual, expected);
  }

  /// Initializes a selector from the given znode bytes, runs a few selections, and returns their results.
  private static List<Map<String, String>> select(String type, boolean upsert, ZkSerializer serializer,
      byte[] idealStateBytes, byte[] externalViewBytes, Set<String> enabledInstances, List<String> segments,
      boolean expectCompact) {
    // Wrap the records as the routing manager does: a compact record is shared, a default one is copied
    ZNRecord idealStateRecord = (ZNRecord) serializer.deserialize(idealStateBytes);
    ZNRecord externalViewRecord = (ZNRecord) serializer.deserialize(externalViewBytes);
    IdealState idealState = HelixHelper.toIdealState(idealStateRecord);
    ExternalView externalView = HelixHelper.toExternalView(externalViewRecord);
    assertEquals(HelixHelper.isCompact(idealStateRecord), expectCompact);
    assertEquals(HelixHelper.isCompact(externalViewRecord), expectCompact);
    if (expectCompact) {
      // The selectors see the immutable outer maps of the parsed records, not copies
      assertSame(idealState.getRecord().getMapFields(), idealStateRecord.getMapFields());
      assertSame(externalView.getRecord().getMapFields(), externalViewRecord.getMapFields());
      assertTrue(idealState.getRecord().getMapFields() instanceof ImmutableSortedArrayMap);
      assertTrue(idealState.getInstanceStateMap("segment_0") instanceof ImmutableSortedArrayMap);
      assertTrue(externalView.getStateMap("segment_0") instanceof ImmutableSortedArrayMap);
    } else {
      assertNotSame(idealState.getRecord().getMapFields(), idealStateRecord.getMapFields());
    }
    Set<String> onlineSegments = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(ONLINE) || entry.getValue().containsValue(CONSUMING)) {
        onlineSegments.add(entry.getKey());
      }
    }

    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(TABLE_NAME);
    when(tableConfig.isUpsertEnabled()).thenReturn(upsert);
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    when(brokerRequest.getPinotQuery()).thenReturn(mock(PinotQuery.class));

    InstanceSelector selector = newSelector(type);
    selector.init(tableConfig, propertyStore, mock(BrokerMetrics.class), null, Clock.systemUTC(), CONFIG,
        enabledInstances, Map.of(), idealState, externalView, onlineSegments);
    List<Map<String, String>> results = new ArrayList<>();
    for (int requestId = 0; requestId < 6; requestId++) {
      InstanceSelector.SelectionResult result = selector.select(brokerRequest, segments, requestId);
      results.add(result.getSegmentToInstanceMap());
      results.add(Map.of("unavailable", String.valueOf(result.getUnavailableSegments())));
    }
    // A second assignment change on the same selector
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    results.add(selector.select(brokerRequest, segments, 0).getSegmentToInstanceMap());
    return results;
  }
}
