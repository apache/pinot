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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import javax.annotation.concurrent.Immutable;


/// The result of one rebuild of the instance selector state (on initialization and on assignment change): the
/// [CandidateGroup]s, and the group of every online segment, old or new, routable or not.
///
/// The only per-segment state is one `int` group id, plus the creation time of the new segments. Everything else is
/// per group, so that an instance change, which only changes which candidates are enabled, never touches per-segment
/// state: this object stays valid, unchanged, until the next rebuild.
///
/// Instances that are a candidate of at least one group get a slot, a dense index that lets per-instance state (enabled
/// flags, per-query segment lists, adaptive ranks) be kept in arrays. A slot is only meaningful for the rebuild that
/// assigned it.
///
/// Never modified once built, so safe to read from any thread once published.
@Immutable
final class SegmentGroups {
  static final int NO_SLOT = -1;

  // Every online segment to the id (index in _groups) of its group, defaultReturnValue(SegmentStates.UNKNOWN_SEGMENT)
  final Object2IntOpenHashMap<String> _segmentToGroupId;
  // Creation time of every segment of the new groups, defaultReturnValue(0) (a creation time is always positive)
  final Object2LongOpenHashMap<String> _newSegmentCreationTimeMap;
  final CandidateGroup[] _groups;
  // Number of segments of each group
  final int[] _groupSizes;
  // Instance of each slot
  final String[] _slotInstances;
  // Pool of each slot
  final int[] _slotPools;
  // Instance to slot, defaultReturnValue(NO_SLOT)
  final Object2IntOpenHashMap<String> _instanceToSlot;

  SegmentGroups(Object2IntOpenHashMap<String> segmentToGroupId,
      Object2LongOpenHashMap<String> newSegmentCreationTimeMap, CandidateGroup[] groups, int[] groupSizes,
      String[] slotInstances, int[] slotPools, Object2IntOpenHashMap<String> instanceToSlot) {
    _segmentToGroupId = segmentToGroupId;
    _newSegmentCreationTimeMap = newSegmentCreationTimeMap;
    _groups = groups;
    _groupSizes = groupSizes;
    _slotInstances = slotInstances;
    _slotPools = slotPools;
    _instanceToSlot = instanceToSlot;
  }

  int getNumGroups() {
    return _groups.length;
  }

  int getNumSlots() {
    return _slotInstances.length;
  }
}
