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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


// Class used to represent the instance state for new segment.
public class SegmentState {
  // List of SegmentInstanceCandidate: which contains instance name and online flags.
  // The candidates have to be in instance sorted order.
  // We store the candidates as a list instead of tree set for memory efficiency.
  // The downside of this approach is that we have to sort the candidates every time we reset the candidates.
  // Since the candidates' size is small, the performance penalty is not much.
  private List<SegmentInstanceCandidate> _candidates;

  // Segment creation time. This could be
  // 1) From ZK if we first see this segment via init call.
  // 2) Use wall time, if first see this segment from onAssignmentChange call.
  // 3) For old segment, we don't need to track creation time anymore, so this will be set to LONG.MIN.
  private long _creationMillis;

  private static class CompareByInstanceName implements Comparator<SegmentInstanceCandidate> {
    public int compare(SegmentInstanceCandidate candidate1, SegmentInstanceCandidate candidate2) {
      return candidate1.getInstance().compareTo(candidate2.getInstance());
    }
  }

  public static SegmentState createSegmentState(long creationMillis) {
    return new SegmentState(creationMillis);
  }

  public static SegmentState createDefaultSegmentState() {
    return new SegmentState(Long.MIN_VALUE);
  }

  private SegmentState(long creationMillis) {
    _creationMillis = creationMillis;
    _candidates = new ArrayList<>();
  }

  public boolean isNew(long nowMillis) {
    return InstanceSelector.isNewSegment(_creationMillis, nowMillis);
  }

  public void promoteToOld() {
    _creationMillis = Long.MIN_VALUE;
  }

  public void resetCandidates(List<SegmentInstanceCandidate> candidates) {
    // Sort the online instances for replica-group routing to work. For multiple segments with the same online
    // instances, if the list is sorted, the same index in the list will always point to the same instance.
    Collections.sort(candidates, new CompareByInstanceName());
    _candidates = candidates;
  }

  public List<SegmentInstanceCandidate> getCandidates() {
    return _candidates;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("creation time millis:");
    builder.append(_creationMillis);
    for (SegmentInstanceCandidate candidate : _candidates) {
      builder.append("[");
      builder.append("Instance:");
      builder.append(candidate.getInstance());
      builder.append("isOnline:");
      builder.append(candidate.isOnline());
      builder.append("]");
    }
    return builder.toString();
  }
}
