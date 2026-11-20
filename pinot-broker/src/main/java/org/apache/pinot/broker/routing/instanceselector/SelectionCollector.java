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

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector.SelectionResult;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.routing.SegmentsToQuery;


/// Collects the selection of one query directly into per-instance segment lists, the shape of the routing table.
///
/// The segment lists of an instance are created on the first segment selected on it, the list of non-optional segments
/// presized from the hints of the [SegmentStates] scaled to the number of queried segments.
///
/// Not thread safe. One instance per query.
public final class SelectionCollector {
  // Smallest initial capacity of a segment list, the default capacity of an ArrayList
  private static final int MIN_INITIAL_CAPACITY = 10;

  private final SegmentStates _segmentStates;
  private final int _numSegments;
  private final boolean _spread;
  private final SegmentsToQuery[] _segmentsToQueryBySlot;
  private final IntArrayList _selectedSlots = new IntArrayList();
  @Nullable
  private List<String> _unavailableSegments;

  /// @param numSegments number of queried segments
  /// @param spread whether the selection spreads the segments of a group over its candidates, rather than selecting
  ///               the same candidate for most segments of a group
  public SelectionCollector(SegmentStates segmentStates, int numSegments, boolean spread) {
    _segmentStates = segmentStates;
    _numSegments = numSegments;
    _spread = spread;
    _segmentsToQueryBySlot = new SegmentsToQuery[segmentStates.getNumSlots()];
  }

  /// Returns the routing of the given segment, or `null` when the segment cannot be routed: it is not tracked yet
  /// (the routing components are updated one after another), or it has no enabled candidate, in which case it is
  /// reported as unavailable when it is an old segment.
  @Nullable
  public GroupRouting resolveRouting(String segment) {
    return resolveRouting(segment, _segmentStates.getGroupId(segment));
  }

  /// Returns the routing of the given segment as [#resolveRouting(String)] does, for a segment whose group id is
  /// already known.
  @Nullable
  public GroupRouting resolveRouting(String segment, int groupId) {
    if (groupId == SegmentStates.UNKNOWN_SEGMENT) {
      return null;
    }
    GroupRouting routing = _segmentStates.getGroupRouting(groupId);
    if (routing.getNumCandidates() == 0) {
      if (_segmentStates.isUnavailable(groupId)) {
        addUnavailable(segment);
      }
      return null;
    }
    return routing;
  }

  /// Selects the candidate at the given index of the given routing for the given segment. The routing must come from
  /// the [SegmentStates] this collector was created with.
  public void add(GroupRouting routing, int candidateIndex, String segment) {
    SegmentsToQuery segmentsToQuery = getSegmentsToQuery(routing.getSlot(candidateIndex));
    // This can only be offline when it is a new segment. And such segment is marked as optional segment so that
    // broker or server can skip it upon any issue to process it.
    if (routing._allOnline || routing.getCandidates().get(candidateIndex).isOnline()) {
      segmentsToQuery.getSegments().add(segment);
    } else {
      segmentsToQuery.getOptionalSegments().add(segment);
    }
  }

  private SegmentsToQuery getSegmentsToQuery(int slot) {
    SegmentsToQuery segmentsToQuery = _segmentsToQueryBySlot[slot];
    if (segmentsToQuery == null) {
      segmentsToQuery = new SegmentsToQuery(new ArrayList<>(getInitialCapacity(slot)), new ArrayList<>());
      _segmentsToQueryBySlot[slot] = segmentsToQuery;
      _selectedSlots.add(slot);
    }
    return segmentsToQuery;
  }

  /// Returns the initial capacity of the list of non-optional segments of the given slot: the share of the queried
  /// segments proportional to the segments the slot gets when all segments are queried, plus slack when spread, raised
  /// to [#MIN_INITIAL_CAPACITY] and capped at the number of queried segments.
  private int getInitialCapacity(int slot) {
    // Without spreading, a selected slot gets all the segments of its groups when they all have the same enabled
    // candidates (mirror assignment). Otherwise, it gets only some of its groups: expect an even spread.
    boolean spread = _spread || !_segmentStates._mirroredBySlot[slot];
    int numSlotSegments =
        spread ? _segmentStates._numSpreadSegmentsBySlot[slot] : _segmentStates._numHostedSegmentsBySlot[slot];
    // The slot is an enabled candidate of a group, so there are routable segments
    long capacity = (long) numSlotSegments * _numSegments / _segmentStates._numRoutableSegments;
    if (spread) {
      // Slack for the variance of the number of segments each candidate gets
      capacity += 4 * (long) Math.sqrt(capacity) + 16;
    }
    return (int) Math.min(Math.max(capacity, MIN_INITIAL_CAPACITY), _numSegments);
  }

  /// Reports the given segment as unavailable.
  public void addUnavailable(String segment) {
    if (_unavailableSegments == null) {
      _unavailableSegments = new ArrayList<>();
    }
    _unavailableSegments.add(segment);
  }

  /// Emits the number of segments selected on each pool, optional segments included.
  public void emitPoolSegQueries(BrokerMetrics brokerMetrics, @Nullable Map<String, String> queryOptions) {
    int numSelectedSlots = _selectedSlots.size();
    if (numSelectedSlots == 0) {
      return;
    }
    Int2IntOpenHashMap numSegmentsByPool = new Int2IntOpenHashMap(2);
    int[] slotPools = _segmentStates._segmentGroups._slotPools;
    for (int i = 0; i < numSelectedSlots; i++) {
      int slot = _selectedSlots.getInt(i);
      SegmentsToQuery segmentsToQuery = _segmentsToQueryBySlot[slot];
      numSegmentsByPool.addTo(slotPools[slot],
          segmentsToQuery.getSegments().size() + segmentsToQuery.getOptionalSegments().size());
    }
    String preferredPoolTag = BrokerMetrics.getTagForPreferredPool(queryOptions);
    for (Int2IntMap.Entry entry : numSegmentsByPool.int2IntEntrySet()) {
      brokerMetrics.addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, entry.getIntValue(), preferredPoolTag,
          String.valueOf(entry.getIntKey()));
    }
  }

  /// Returns the selection result. The collector must not be used afterwards.
  public SelectionResult build() {
    int numSelectedSlots = _selectedSlots.size();
    Map<String, SegmentsToQuery> instanceToSegmentsMap = Maps.newHashMapWithExpectedSize(numSelectedSlots);
    String[] slotInstances = _segmentStates._segmentGroups._slotInstances;
    for (int i = 0; i < numSelectedSlots; i++) {
      int slot = _selectedSlots.getInt(i);
      instanceToSegmentsMap.put(slotInstances[slot], _segmentsToQueryBySlot[slot]);
    }
    List<String> unavailableSegments = _unavailableSegments != null ? _unavailableSegments : List.of();
    return new SelectionResult(instanceToSegmentsMap, unavailableSegments, 0);
  }
}
