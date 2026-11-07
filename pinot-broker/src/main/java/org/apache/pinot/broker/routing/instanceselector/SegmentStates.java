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

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;
import javax.annotation.concurrent.Immutable;


/// The routing state of a table read by the query threads: the candidate group of every online segment (see
/// [SegmentGroups]), and how each group is routed under the enabled instances (see [GroupRouting]).
///
/// For old segments, the candidates are always online, and a segment without enabled candidate is unavailable. For
/// new segments, the online flag of a candidate tells whether the instance is online, and a segment is never reported
/// as unavailable, because it is valid for a new segment to be offline.
///
/// Published as a whole through one volatile write, and read once per query, so that the group of every segment, the
/// routing of every group and the state derived from them stay consistent with each other. Never modified once
/// published.
@Immutable
public final class SegmentStates {
  /// Group id of a segment that is not tracked, e.g. because the routing components are updated one after another.
  public static final int UNKNOWN_SEGMENT = -1;

  final SegmentGroups _segmentGroups;
  // Routing of each group under the enabled instances
  final GroupRouting[] _routings;
  final boolean[] _enabledBySlot;
  private final Set<String> _servingInstances;
  private final TableReplicaHealth _replicaHealth;
  // Number of segments with at least one enabled candidate. With the per-slot fields below, a hint to presize the
  // per-query segment lists of each slot.
  final int _numRoutableSegments;
  // Number of segments of the groups the slot is an enabled candidate of
  final int[] _numHostedSegmentsBySlot;
  // Whether the groups the slot is an enabled candidate of all have the same enabled candidates (mirror assignment)
  final boolean[] _mirroredBySlot;
  // Number of segments the slot gets when every group spreads its segments evenly over its enabled candidates
  final int[] _numSpreadSegmentsBySlot;

  SegmentStates(SegmentGroups segmentGroups, GroupRouting[] routings, boolean[] enabledBySlot,
      TableReplicaHealth replicaHealth) {
    _segmentGroups = segmentGroups;
    _routings = routings;
    _enabledBySlot = enabledBySlot;
    _replicaHealth = replicaHealth;

    // Every slot is a candidate of a group, so the enabled slots are the union of the enabled candidates
    int numSlots = segmentGroups.getNumSlots();
    Set<String> servingInstances = Sets.newHashSetWithExpectedSize(numSlots);
    for (int slot = 0; slot < numSlots; slot++) {
      if (enabledBySlot[slot]) {
        servingInstances.add(segmentGroups._slotInstances[slot]);
      }
    }
    _servingInstances = servingInstances;

    int numRoutableSegments = 0;
    int[] numHostedSegmentsBySlot = new int[numSlots];
    boolean[] mirroredBySlot = new boolean[numSlots];
    Arrays.fill(mirroredBySlot, true);
    // Enabled slots of the first group each slot is an enabled candidate of
    int[][] firstGroupSlotsBySlot = new int[numSlots][];
    double[] numSpreadSegmentsBySlot = new double[numSlots];
    int numGroups = routings.length;
    for (int groupId = 0; groupId < numGroups; groupId++) {
      GroupRouting routing = routings[groupId];
      int numCandidates = routing.getNumCandidates();
      if (numCandidates > 0) {
        int groupSize = segmentGroups._groupSizes[groupId];
        numRoutableSegments += groupSize;
        int[] slots = routing._slots;
        double numSegmentsPerCandidate = (double) groupSize / numCandidates;
        for (int slot : slots) {
          numHostedSegmentsBySlot[slot] += groupSize;
          int[] firstGroupSlots = firstGroupSlotsBySlot[slot];
          if (firstGroupSlots == null) {
            firstGroupSlotsBySlot[slot] = slots;
          } else if (mirroredBySlot[slot] && !Arrays.equals(firstGroupSlots, slots)) {
            mirroredBySlot[slot] = false;
          }
          numSpreadSegmentsBySlot[slot] += numSegmentsPerCandidate;
        }
      }
    }
    _numRoutableSegments = numRoutableSegments;
    _numHostedSegmentsBySlot = numHostedSegmentsBySlot;
    _mirroredBySlot = mirroredBySlot;
    _numSpreadSegmentsBySlot = new int[numSlots];
    for (int slot = 0; slot < numSlots; slot++) {
      _numSpreadSegmentsBySlot[slot] = (int) Math.round(numSpreadSegmentsBySlot[slot]);
    }
  }

  /// Returns the id of the group of the given segment, or [#UNKNOWN_SEGMENT] if the segment is not tracked.
  public int getGroupId(String segment) {
    return _segmentGroups._segmentToGroupId.getInt(segment);
  }

  /// Returns the routing of the given group under the enabled instances.
  public GroupRouting getGroupRouting(int groupId) {
    return _routings[groupId];
  }

  /// Returns whether the segments of the given group are unavailable: old segments without enabled candidate.
  public boolean isUnavailable(int groupId) {
    return _routings[groupId].getNumCandidates() == 0 && !_segmentGroups._groups[groupId]._new;
  }

  int getNumSlots() {
    return _enabledBySlot.length;
  }

  /// Returns the slot of the given instance, or [SegmentGroups#NO_SLOT] if it is not a candidate of any group.
  int getSlot(String instance) {
    return _segmentGroups._instanceToSlot.getInt(instance);
  }

  /// Returns the enabled instances serving the table.
  public Set<String> getServingInstances() {
    return _servingInstances;
  }

  /// Returns how well the segments are replicated across the enabled instances.
  public TableReplicaHealth getReplicaHealth() {
    return _replicaHealth;
  }
}
