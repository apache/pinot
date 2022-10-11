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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * Interface for segment assignment strategies
 */
public interface SegmentAssignmentStrategy {

  /**
   * Initializes the segment assignment strategy.
   *
   * @param helixManager Helix manager
   * @param tableConfig Table config
   */
  void init(HelixManager helixManager, TableConfig tableConfig);

  /**
   * Assigns segment to instances. The assignment strategy will be configured in
   * OfflineSegmentAssignment and RealtimeSegmentAssignment classes and depending on type of
   * assignment strategy, this function will be called to assign a new segment
   *
   * @param segmentName Name of the segment to be assigned
   * @param currentAssignment Current segment assignment of the table (map from segment name to instance state map)
   * @param instancePartitions Instance partitions
   * @return List of instances to assign the segment to
   */
  List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType);

  /**
   * Re-assigns segment to instances. The assignment strategy will be configured in
   * OfflineSegmentAssignment and RealtimeSegmentAssignment classes and depending on type of
   * assignment strategy, this function will be called to re-assign a segment
   * when the InstancePartitions has been changed.
   *
   * @param currentAssignment Current segment assignment of the table (map from segment name to instance state map)
   * @param instancePartitions Instance partitions
   * @return Rebalanced assignment for the segments per assignment strategy
   */
  Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType);
}
