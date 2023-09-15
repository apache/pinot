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


/**
 * Contains the push time and candidate instances for a new segment.
 */
@Immutable
public class NewSegmentState {
  // Segment creation time. This could be
  // 1) From ZK if we first see this segment via init call.
  // 2) Use wall time if we first see this segment from onAssignmentChange call.
  private final long _creationTimeMs;

  // List of SegmentInstanceCandidate: which contains instance name and online flags.
  // The candidates have to be in instance sorted order.
  private final List<SegmentInstanceCandidate> _candidates;

  public NewSegmentState(long creationTimeMs, List<SegmentInstanceCandidate> candidates) {
    _creationTimeMs = creationTimeMs;
    _candidates = candidates;
  }

  public long getCreationTimeMs() {
    return _creationTimeMs;
  }

  public List<SegmentInstanceCandidate> getCandidates() {
    return _candidates;
  }
}
