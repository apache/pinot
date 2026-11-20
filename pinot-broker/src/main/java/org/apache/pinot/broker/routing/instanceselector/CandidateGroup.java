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

import java.util.List;
import javax.annotation.concurrent.Immutable;


/// One distinct candidate list of a rebuild, shared by all the segments routed the same way: segments with the same
/// candidates (instance, online flag, pool and ideal state replica id, in instance order), the same number of expected
/// replicas, and of the same kind (old or new) belong to the same group.
///
/// The candidates include disabled instances. [#filterEnabled] derives the [GroupRouting] under the enabled
/// instances. Never modified once built.
@Immutable
final class CandidateGroup {
  // All the candidates, disabled ones included
  final GroupRouting _routing;
  // New segments are never reported as unavailable, and are not measured by the replica health
  final boolean _new;
  // Number of instances in ONLINE/CONSUMING state in the ideal state (at least 1) for old segments, 0 for new segments
  final int _expectedReplicas;
  // One segment of the group, for logging
  final String _sampleSegment;

  CandidateGroup(GroupRouting routing, boolean isNew, int expectedReplicas, String sampleSegment) {
    _routing = routing;
    _new = isNew;
    _expectedReplicas = expectedReplicas;
    _sampleSegment = sampleSegment;
  }

  /// Returns the routing of the group under the given enabled slots: the routing over all the candidates when every
  /// candidate is enabled, otherwise a routing over the enabled candidates that shares their
  /// [SegmentInstanceCandidate] objects.
  GroupRouting filterEnabled(boolean[] enabledBySlot) {
    int[] slots = _routing._slots;
    int numCandidates = slots.length;
    int numEnabledCandidates = 0;
    for (int slot : slots) {
      if (enabledBySlot[slot]) {
        numEnabledCandidates++;
      }
    }
    if (numEnabledCandidates == numCandidates) {
      return _routing;
    }
    if (numEnabledCandidates == 0) {
      return GroupRouting.EMPTY;
    }
    SegmentInstanceCandidate[] enabledCandidates = new SegmentInstanceCandidate[numEnabledCandidates];
    int[] enabledSlots = new int[numEnabledCandidates];
    int index = 0;
    for (int i = 0; i < numCandidates; i++) {
      int slot = slots[i];
      if (enabledBySlot[slot]) {
        enabledCandidates[index] = _routing.getCandidates().get(i);
        enabledSlots[index] = slot;
        index++;
      }
    }
    return new GroupRouting(List.of(enabledCandidates), enabledSlots);
  }
}
