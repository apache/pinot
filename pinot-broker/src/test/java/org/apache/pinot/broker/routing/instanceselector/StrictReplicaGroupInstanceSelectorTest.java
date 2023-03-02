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
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class StrictReplicaGroupInstanceSelectorTest {
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

  private void createSegment(String segment, long creationMillis) {
    SegmentZKMetadata offlineSegmentZKMetadata0 = new SegmentZKMetadata(segment);
    offlineSegmentZKMetadata0.setCreationTime(creationMillis);
    offlineSegmentZKMetadata0.setTimeUnit(TimeUnit.MILLISECONDS);
    ZNRecord segmentZKMetadataZNRecord0 = offlineSegmentZKMetadata0.toZNRecord();
    Mockito.when(
        _propertyStore.get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE_NAME, segment)), any(),
            anyInt())).thenReturn(segmentZKMetadataZNRecord0);
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
    // Set segment0 as old segment
    String oldSeg = "segment0";
    createSegment(oldSeg,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    // Set segment1 as new segment
    String newSeg = "segment1";
    createSegment(newSeg, _mutableClock.millis() - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = Map.ofEntries(Map.entry(oldSeg, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1)));
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of(Pair.of(instance1, ONLINE)))
    );
    ExternalView externalView = createExternalView(externalViewMap);

    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance0 for newSeg, and we exclude it and not report error.
    // We don't mark instance0 as unavailable for segment0 either.
    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance0)
    );
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Second selection, we select instance0 for newSeg, we serve the newSeg and not report error;
    requestId = 1;
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance1),
        Map.entry(newSeg, instance1)
    );
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    // For first selection, newSeg will be served from instance1 since now.
    requestId = 0;
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance0),
        Map.entry(newSeg, instance1)
    );
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    // For second selection, newSeg will still be served from instance1.
    requestId = 1;
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance1),
        Map.entry(newSeg, instance1)
    );
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    // Upon re-initialization, newly old segments will start marking unavailable instances.
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    requestId = 0;
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance1),
        Map.entry(newSeg, instance1)
    );
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Test that we don't report new segment as unavailable till it gets old.
  @Test
  public void testNewSegmentFromZKMetadataReportingUnavailable() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    createSegment(oldSeg,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    // Set segment1 as new segment
    String newSeg = "segment1";
    createSegment(newSeg, _mutableClock.millis() - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1))
    );
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of())
    );
    ExternalView externalView = createExternalView(externalViewMap);

    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(Map.entry(oldSeg, instance0));
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment and we see newSeg is reported as unavailable segment.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    // For first selection, newSeg will be served from instance1 since now.
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(Map.entry(oldSeg, instance0));
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
  }

  // Test that we mark new segment as old when we see error state.
  @Test
  public void testNewSegmentGetsOldWithErrorState() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    createSegment(oldSeg,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    // Set segment1 as new segment
    String newSeg = "segment1";
    createSegment(newSeg, _mutableClock.millis() - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1))
    );
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of())
    );
    ExternalView externalView = createExternalView(externalViewMap);

    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(Map.entry(oldSeg, instance0));
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Report error instance for segment1 since segment1 becomes old and we should report the error.
    externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of(Pair.of(instance0, ERROR)))
    );
    externalView = createExternalView(externalViewMap);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries();
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg, newSeg));

    // Get segment1 back online in instance1
    externalViewMap = Map.ofEntries(Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of(Pair.of(instance0, ERROR), Pair.of(instance1, ONLINE))));
    externalView = createExternalView(externalViewMap);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    expectedReplicaGroupInstanceSelectorResult =
        Map.ofEntries(Map.entry(oldSeg, instance1), Map.entry(newSeg, instance1));
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Test that we mark new segment as old when external view state converges with ideal state.
  @Test
  public void testNewSegmentGetsOldWithStateConverge() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    createSegment(oldSeg,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    // Set segment1 as new segment
    String newSeg = "segment1";
    createSegment(newSeg, _mutableClock.millis() - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1))
    );
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of())
    );
    ExternalView externalView = createExternalView(externalViewMap);

    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(Map.entry(oldSeg, instance0));
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Segment1 is not old anymore with state converge.
    externalViewMap = Map.ofEntries(Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))));
    externalView = createExternalView(externalViewMap);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Segment1 becomes unavailable.
    externalViewMap = Map.ofEntries(Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of()));
    externalView = createExternalView(externalViewMap);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries();
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg, newSeg));
  }

  // Test that we get new segment from ideal state update.
  @Test
  public void testNewSegmentsFromIDWithMissingEV() {
    String oldSeg0 = "segment0";
    createSegment(oldSeg0,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    String oldSeg1 = "segment1";
    createSegment(oldSeg1,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1);

    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);

    Map<String, List<String>> idealSateMap = Map.ofEntries(
        Map.entry(oldSeg0, List.of(instance0, instance1)),
        Map.entry(oldSeg1, List.of(instance0, instance1))
    );
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(oldSeg1, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)))
    );
    ExternalView externalView = createExternalView(externalViewMap);
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Add a new segment to ideal state with missing external view.
    String newSeg = "segment2";
    onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1, newSeg);
    idealSateMap = Map.ofEntries(
        Map.entry(oldSeg0, List.of(instance0, instance1)),
        Map.entry(oldSeg1, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1))
    );
    idealState = createIdealState(idealSateMap);
    externalViewMap = Map.ofEntries(
        Map.entry(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(oldSeg1, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)))
    );
    externalView = createExternalView(externalViewMap);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg0, instance0),
        Map.entry(oldSeg1, instance0)
    );
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment.
    _mutableClock.add(CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS + 10, ChronoUnit.MILLIS);
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));

    // On state update, all segments become unavailable.
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries();
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg0, oldSeg1, newSeg));
  }

  // Test that on instance change, we exclude not enabled instance from serving for new segments.
  @Test
  public void testExcludeNotEnabledInstanceForNewSegment() {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    createSegment(oldSeg,
        _mutableClock.millis() - CommonConstants.Helix.StateModel.NEW_SEGMENT_EXPIRATION_MILLIS - 100);
    // Set segment1 as new segment
    String newSeg = "segment1";
    createSegment(newSeg, _mutableClock.millis() - 100);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<String>> idealSateMap = Map.ofEntries(Map.entry(oldSeg, List.of(instance0, instance1)),
        Map.entry(newSeg, List.of(instance0, instance1)));
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online]
    Map<String, List<Pair<String, String>>> externalViewMap = Map.ofEntries(
        Map.entry(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))),
        Map.entry(newSeg, List.of(Pair.of(instance0, ONLINE)))
    );
    ExternalView externalView = createExternalView(externalViewMap);

    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(TABLE_NAME, _brokerMetrics, null, _propertyStore, _mutableClock);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance0 for newSeg.
    int requestId = 0;
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance0),
        Map.entry(newSeg, instance0)
    );
    InstanceSelector.SelectionResult selectionResult =
        strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Remove instance0 from enabledInstances.
    enabledInstances = ImmutableSet.of(instance1);
    List<String> changeInstance = ImmutableList.of(instance0);
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, changeInstance);
    selectionResult = strictReplicaGroupInstanceSelector.select(_brokerRequest, List.copyOf(onlineSegments), requestId);
    // We don't include instance0 in selection anymore.
    expectedReplicaGroupInstanceSelectorResult = Map.ofEntries(
        Map.entry(oldSeg, instance1)
    );
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }
}
