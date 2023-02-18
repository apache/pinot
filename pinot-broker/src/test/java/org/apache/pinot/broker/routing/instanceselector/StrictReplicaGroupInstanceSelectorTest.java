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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class StrictReplicaGroupInstanceSelectorTest {
  private final static List<String> SEGMENTS = Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4",
      "segment5", "segment6", "segment7", "segment8", "segment9", "segment10", "segment11");

  @Test
  public void testReplicaGroupInstanceSelectorNewSegment() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    // numReplicas = 3, fanning the query to 2 replica groups
    queryOptions.put("numReplicaGroupsToQuery", "2");
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);
    AdaptiveServerSelector adaptiveServerSelector = null;

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    // New segment, no instance is online in external view.
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
    }


    List<String> segments = getSegments();
    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment: segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorOldSegmentWithoutFullReplica() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    // numReplicas = 3, fanning the query to 2 replica groups
    queryOptions.put("numReplicaGroupsToQuery", "2");
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);
    AdaptiveServerSelector adaptiveServerSelector = null;

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    // Old segment but it only has one replica.
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
    }
    externalViewInstanceStateMap0.put(instance0, ONLINE);

    List<String> segments = getSegments();
    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment: segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertFalse(selectionResult.getSegmentToInstanceMap().isEmpty());

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (String segment: segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance0);
    }
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  private static List<String> getSegments() {
    return SEGMENTS;
  }
}
