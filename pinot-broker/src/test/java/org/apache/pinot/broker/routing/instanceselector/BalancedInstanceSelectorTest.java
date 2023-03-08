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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.threeten.extra.MutableClock;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BalancedInstanceSelectorTest {
  private AutoCloseable _mocks;

  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private BrokerRequest _brokerRequest;

  @Mock
  private PinotQuery _pinotQuery;

  private MutableClock _mutableClock;

  private void createSegments(List<Pair<String, Long>> segmentCreationMillis) {
    List<String> segmentZKMetadataPaths = new ArrayList<>();
    List<ZNRecord> zkRecords = new ArrayList<>();
    for (Pair<String, Long> segment : segmentCreationMillis) {
      SegmentZKMetadata offlineSegmentZKMetadata0 = new SegmentZKMetadata(segment.getLeft());
      offlineSegmentZKMetadata0.setCreationTime(segment.getRight());
      offlineSegmentZKMetadata0.setTimeUnit(TimeUnit.MILLISECONDS);
      ZNRecord record = offlineSegmentZKMetadata0.toZNRecord();
      segmentZKMetadataPaths.add(
          ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE_NAME, segment.getLeft()));
      zkRecords.add(record);
    }
    Mockito.when(_propertyStore.get(eq(segmentZKMetadataPaths), any(), anyInt(), anyBoolean())).thenReturn(zkRecords);
  }

  private IdealState createIdealState(Map<String, List<String>> onlineInstances) {
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    for (Map.Entry<String, List<String>> entry : onlineInstances.entrySet()) {
      Map<String, String> idealStateInstanceStateMap = new TreeMap<>();
      for (String instance : entry.getValue()) {
        idealStateInstanceStateMap.put(instance, ONLINE);
      }
      idealStateSegmentAssignment.put(entry.getKey(), idealStateInstanceStateMap);
    }
    return idealState;
  }

  private ExternalView createExternalView(Map<String, List<Pair<String, String>>> segmentState) {
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    for (Map.Entry<String, List<Pair<String, String>>> entry : segmentState.entrySet()) {
      Map<String, String> externalViewInstanceStateMap = new TreeMap<>();
      for (Pair<String, String> instanceState : entry.getValue()) {
        externalViewInstanceStateMap.put(instanceState.getLeft(), instanceState.getRight());
      }
      externalViewSegmentAssignment.put(entry.getKey(), externalViewInstanceStateMap);
    }
    return externalView;
  }

  public static final String TABLE_NAME = "testTable_OFFLINE";

  @BeforeMethod
  public void setUp() {
    _mutableClock = MutableClock.of(Instant.now(), ZoneId.systemDefault());
    _mocks = MockitoAnnotations.openMocks(this);
    when(_brokerRequest.getPinotQuery()).thenReturn(_pinotQuery);
    when(_pinotQuery.getQueryOptions()).thenReturn(null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // Test that new segment from zk metadata will not hotspot the only online server.
  // And we will mark the new segment as old upon time change.
  // With re-initialization, unavailable instances will be updated.
  @Test
  public void testNewSegmentFromZKMetadataSelection() {
    String oldSeg = "segment0";
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance1, ONLINE)));
    }};
    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance0 for newSeg, and we exclude it and not report error.
    // We don't mark instance0 as unavailable for segment0 either.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance0);
      put(newSeg, instance1);
    }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Second selection, we select instance0 for newSeg, we serve the newSeg and not report error;
    requestId = 1;
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance1);
    }};
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    // Upon re-initialization, newly old segments can only serve from online instance
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    requestId = 0;
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance0);
      put(newSeg, instance1);
    }};
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    requestId = 1;
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance1);
      put(newSeg, instance1);
    }};
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Test that we don't report new segment as unavailable till it gets old.
  @Test
  public void testNewSegmentFromZKMetadataReportingUnavailable() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of());
    }};
    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
          put(oldSeg, instance0);
        }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment and we see newSeg is reported as unavailable segment.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
  }

  // Test that we mark new segment as old when we see error state.
  @Test
  public void testNewSegmentGetsOldWithErrorState() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of());
    }};

    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
          put(oldSeg, instance0);
        }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Report error instance for segment1 since segment1 becomes old and we should report the error.
    externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance0, ERROR)));
    }};
    externalView = createExternalView(externalViewMap);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));

    // Get segment1 back online in instance1
    externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance0, ERROR), Pair.of(instance1, ONLINE)));
    }};
    externalView = createExternalView(externalViewMap);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance0);
      put(newSeg, instance1);
    }};
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Test that we mark new segment as old when external view state converges with ideal state.
  @Test
  public void testNewSegmentGetsOldWithStateConverge() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of());
    }};
    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance0);
    }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Segment1 is not old anymore with state converge.
    externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
    }};
    externalView = createExternalView(externalViewMap);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Segment1 becomes unavailable.
    externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of());
    }};
    externalView = createExternalView(externalViewMap);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
  }

  // Test that we get new segment from ideal state update.
  @Test
  public void testNewSegmentsFromIDWithMissingEV() {
    String oldSeg0 = "segment0";
    String oldSeg1 = "segment1";
    Set<String> onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1);

    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);

    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg0, ImmutableList.of(instance0, instance1));
      put(oldSeg1, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(oldSeg1, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
    }};
    ExternalView externalView = createExternalView(externalViewMap);
    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Add a new segment to ideal state with missing external view.
    String newSeg = "segment2";
    onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1, newSeg);
    idealSateMap = new HashMap<>() {{
      put(oldSeg0, ImmutableList.of(instance0, instance1));
      put(oldSeg1, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};

    idealState = createIdealState(idealSateMap);
    externalViewMap = new HashMap<>() {{
      put(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(oldSeg1, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
    }};
    externalView = createExternalView(externalViewMap);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg0, instance0);
      put(oldSeg1, instance1);
    }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment.
    // On state update, all segments become unavailable.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
  }

  // Test that on instance change, we exclude not enabled instance from serving for new segments.
  @Test
  public void testExcludeNotEnabledInstanceForNewSegment() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online]
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance0, ONLINE)));
    }};

    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance1 for newSeg.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance0);
    }};
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    requestId = 1;
    // Second selection, we select instance0 for newSeg.
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance1);
      put(newSeg, instance0);
    }};
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Remove instance0 from enabledInstances.
    enabledInstances = ImmutableSet.of(instance1);
    List<String> changeInstance = ImmutableList.of(instance0);
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, changeInstance);
    selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    // We don't include instance0 in selection anymore.
    expectedBalancedInstanceSelectorResult = new HashMap<>() {{
      put(oldSeg, instance1);
    }};
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testExcludeInstanceNotInIdealState() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTime = ImmutableList.of(
        Pair.of(oldSeg, _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100),
        Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTime);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1, instance2);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = new HashMap() {{
      put(oldSeg, ImmutableList.of(instance0, instance1));
      put(newSeg, ImmutableList.of(instance0, instance1));
    }};
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance2: online]
    //   [segment1] -> [instance2: online]
    Map<String, List<Pair<String, String>>> externalViewMap = new HashMap<>() {{
      put(oldSeg, ImmutableList.of(Pair.of(instance2, ONLINE)));
      put(newSeg, ImmutableList.of(Pair.of(instance2, ONLINE)));
    }};

    ExternalView externalView = createExternalView(externalViewMap);

    BalancedInstanceSelector replicaGroupInstanceSelector =
        new BalancedInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // No selection because the external view is not in ideal state.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>();
    InstanceSelector.SelectionResult selectionResult =
        replicaGroupInstanceSelector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg));
  }
}
