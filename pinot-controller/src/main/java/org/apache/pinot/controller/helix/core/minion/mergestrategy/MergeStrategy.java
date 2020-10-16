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
package org.apache.pinot.controller.helix.core.minion.mergestrategy;

import java.util.List;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;


/**
 * Merge strategy interface
 */
public interface MergeStrategy {

  /**
   * Generate the merge task candidates. Given the list of segments to merge and the max number of segments per task
   * configuration, this function computes the list of merge task candidates that are satisfying the conditions.
   *
   * The first level list represents each task and the second list represents the list of segments that will be
   * merged together.
   *
   * @param segmentsToMerge
   * @param maxNumSegmentsPerTask
   * @return A list of a set of segments that needs to be scheduled
   */
  List<List<SegmentZKMetadata>> generateMergeTaskCandidates(List<SegmentZKMetadata> segmentsToMerge,
      int maxNumSegmentsPerTask);
}
