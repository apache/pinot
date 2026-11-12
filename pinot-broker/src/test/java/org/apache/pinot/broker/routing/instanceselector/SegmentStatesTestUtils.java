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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/// Builds and inspects [SegmentStates] in tests.
public class SegmentStatesTestUtils {
  private SegmentStatesTestUtils() {
  }

  private static final int NO_GROUP = -1;

  /// Returns the states of the given segments, each with the given candidates (all enabled), plus the given unavailable
  /// segments without candidate. Segments given the same list object share a candidate group, the way segments with
  /// the same candidates do after a rebuild. A group with an offline candidate is a group of new segments, any other
  /// group is a group of old segments. All the candidates of an instance must have the same pool.
  ///
  /// The replica health is measured the way a rebuild measures it. The new segments have no creation time, so the
  /// states are meant for selection: a rebuild from them (`onAssignmentChange`) would treat these segments as old.
  public static SegmentStates createSegmentStates(Map<String, List<SegmentInstanceCandidate>> candidatesMap,
      Set<String> unavailableSegments) {
    Object2IntOpenHashMap<String> segmentToGroupId =
        new Object2IntOpenHashMap<>(candidatesMap.size() + unavailableSegments.size());
    segmentToGroupId.defaultReturnValue(SegmentStates.UNKNOWN_SEGMENT);
    Object2IntOpenHashMap<String> instanceToSlot = new Object2IntOpenHashMap<>();
    instanceToSlot.defaultReturnValue(SegmentGroups.NO_SLOT);
    List<String> slotInstances = new ArrayList<>();
    IntArrayList slotPools = new IntArrayList();
    // Keyed by candidate list identity
    Reference2IntOpenHashMap<List<SegmentInstanceCandidate>> groupIds = new Reference2IntOpenHashMap<>();
    groupIds.defaultReturnValue(NO_GROUP);
    List<CandidateGroup> groups = new ArrayList<>();
    IntArrayList groupSizes = new IntArrayList();
    for (Map.Entry<String, List<SegmentInstanceCandidate>> entry : candidatesMap.entrySet()) {
      String segment = entry.getKey();
      List<SegmentInstanceCandidate> candidates = entry.getValue();
      int groupId = groupIds.getInt(candidates);
      if (groupId == NO_GROUP) {
        groupId = groups.size();
        int numCandidates = candidates.size();
        int[] slots = new int[numCandidates];
        boolean isNew = false;
        for (int i = 0; i < numCandidates; i++) {
          SegmentInstanceCandidate candidate = candidates.get(i);
          String instance = candidate.getInstance();
          int slot = instanceToSlot.getInt(instance);
          if (slot == SegmentGroups.NO_SLOT) {
            slot = slotInstances.size();
            instanceToSlot.put(instance, slot);
            slotInstances.add(instance);
            slotPools.add(candidate.getPool());
          } else {
            Preconditions.checkArgument(slotPools.getInt(slot) == candidate.getPool(),
                "Instance: %s has candidates in different pools", instance);
          }
          slots[i] = slot;
          isNew |= !candidate.isOnline();
        }
        groups.add(new CandidateGroup(new GroupRouting(candidates, slots), isNew,
            isNew ? 0 : Math.max(1, numCandidates), segment));
        groupSizes.add(0);
        groupIds.put(candidates, groupId);
      }
      segmentToGroupId.put(segment, groupId);
      groupSizes.set(groupId, groupSizes.getInt(groupId) + 1);
    }
    if (!unavailableSegments.isEmpty()) {
      int groupId = groups.size();
      groups.add(new CandidateGroup(GroupRouting.EMPTY, false, 1, unavailableSegments.iterator().next()));
      groupSizes.add(unavailableSegments.size());
      for (String segment : unavailableSegments) {
        segmentToGroupId.put(segment, groupId);
      }
    }

    int numGroups = groups.size();
    GroupRouting[] routings = new GroupRouting[numGroups];
    // Every candidate is enabled, so every measured segment is fully replicated
    int numMeasuredSegments = 0;
    int numUnavailableSegments = 0;
    for (int groupId = 0; groupId < numGroups; groupId++) {
      CandidateGroup group = groups.get(groupId);
      routings[groupId] = group._routing;
      if (!group._new) {
        if (TableReplicaHealth.shouldMeasure(group._expectedReplicas)) {
          numMeasuredSegments += groupSizes.getInt(groupId);
        }
        if (group._routing.getNumCandidates() == 0) {
          numUnavailableSegments += groupSizes.getInt(groupId);
        }
      }
    }
    SegmentGroups segmentGroups = new SegmentGroups(segmentToGroupId, new Object2LongOpenHashMap<>(),
        groups.toArray(new CandidateGroup[0]), groupSizes.toIntArray(), slotInstances.toArray(new String[0]),
        slotPools.toIntArray(), instanceToSlot);
    boolean[] enabledBySlot = new boolean[slotInstances.size()];
    Arrays.fill(enabledBySlot, true);
    return new SegmentStates(segmentGroups, routings, enabledBySlot,
        new TableReplicaHealth(TableReplicaHealth.FULLY_REPLICATED_PERCENT, numMeasuredSegments,
            numUnavailableSegments));
  }

  /// Returns the enabled candidates of the given segment, or `null` if the segment is not tracked or has no enabled
  /// candidate.
  @Nullable
  public static List<SegmentInstanceCandidate> getCandidates(SegmentStates segmentStates, String segment) {
    int groupId = segmentStates.getGroupId(segment);
    if (groupId == SegmentStates.UNKNOWN_SEGMENT) {
      return null;
    }
    List<SegmentInstanceCandidate> candidates = segmentStates.getGroupRouting(groupId).getCandidates();
    return !candidates.isEmpty() ? candidates : null;
  }

  /// Returns whether the segments of the given group are new segments.
  public static boolean isNew(SegmentStates segmentStates, int groupId) {
    return segmentStates._segmentGroups._groups[groupId]._new;
  }
}
