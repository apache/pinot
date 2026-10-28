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


/// The candidates of one candidate group, all of them or only the enabled ones, in instance order, along with the
/// slot (see [SegmentGroups]) of each candidate.
///
/// Never modified once built, so it can be shared by any number of segments and published states.
@Immutable
public final class GroupRouting {
  static final GroupRouting EMPTY = new GroupRouting(List.of(), new int[0]);

  final List<SegmentInstanceCandidate> _candidates;
  final int[] _slots;
  // False only for a new segment group with a candidate that is not online yet
  final boolean _allOnline;

  GroupRouting(List<SegmentInstanceCandidate> candidates, int[] slots) {
    _candidates = candidates;
    _slots = slots;
    boolean allOnline = true;
    for (SegmentInstanceCandidate candidate : candidates) {
      if (!candidate.isOnline()) {
        allOnline = false;
        break;
      }
    }
    _allOnline = allOnline;
  }

  /// Returns the candidates in instance order.
  public List<SegmentInstanceCandidate> getCandidates() {
    return _candidates;
  }

  public int getNumCandidates() {
    return _slots.length;
  }

  /// Returns the slot of the candidate at the given index.
  int getSlot(int candidateIndex) {
    return _slots[candidateIndex];
  }
}
