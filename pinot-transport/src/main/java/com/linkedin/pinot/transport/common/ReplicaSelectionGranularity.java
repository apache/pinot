/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.common;

/**
 * Determines at what level the selection for nodes (replica) has to happen
 */
public enum ReplicaSelectionGranularity {
  /**
   * For each segmentId in the request, the replica-selection policy is applied to get the node.
   * If the selection policy is random or round-robin, then this granularity would likely increase
   * the fan-out of the scatter request since there is a greater chance all replicas will be queried
   **/
  SEGMENT_ID,
  /**
   * For each segmentId-group in the request, the replica-selection policy is applied to get the node.
   * This will likely reduce the fan-out as all segmentIds in the segmentId group goes to the same service.
   * This is assuming the services hosting each segmentId-groups are disjoint.
   **/
  SEGMENT_ID_SET
}
