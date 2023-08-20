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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;


/**
 * The {@code SegmentStates} contains the candidate instances for each segment, and the unavailable segments for routing
 * purpose.
 *
 * For old segments, the instance candidates should always have online flag set to true.
 * For old segments without any enabled instance candidates, we report them as unavailable segments.
 *
 * For new segments, the online flag within the instance candidates indicates whether the instance is online or not.
 * We don't report new segments as unavailable segments because it is valid for new segments to be offline.
 */
@Immutable
public class SegmentStates {
  private final Map<String, List<SegmentInstanceCandidate>> _instanceCandidatesMap;
  private final Set<String> _servingInstances;
  private final Set<String> _unavailableSegments;

  public SegmentStates(Map<String, List<SegmentInstanceCandidate>> instanceCandidatesMap, Set<String> servingInstances,
      Set<String> unavailableSegments) {
    _instanceCandidatesMap = instanceCandidatesMap;
    _servingInstances = servingInstances;
    _unavailableSegments = unavailableSegments;
  }

  @Nullable
  public List<SegmentInstanceCandidate> getCandidates(String segment) {
    return _instanceCandidatesMap.get(segment);
  }

  public Set<String> getServingInstances() {
    return _servingInstances;
  }

  public Set<String> getUnavailableSegments() {
    return _unavailableSegments;
  }
}
