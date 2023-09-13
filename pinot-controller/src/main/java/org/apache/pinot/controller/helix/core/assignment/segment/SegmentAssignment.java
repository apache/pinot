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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * Interface for segment assignment and table rebalance.
 */
public interface SegmentAssignment {

  /**
   * Initializes the segment assignment.
   * @param helixManager Helix manager
   * @param tableConfig Table config
   */
  void init(HelixManager helixManager, TableConfig tableConfig);

  /**
   * Assigns segment to instances.
   *
   * @param segmentName Name of the segment to be assigned
   * @param currentAssignment Current segment assignment of the table (map from segment name to instance state map)
   * @param instancePartitionsMap Map from type (OFFLINE|CONSUMING|COMPLETED) to instance partitions
   * @return List of instances to assign the segment to
   */
  List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap);

  /**
   * Rebalances the segment assignment for a table.
   *
   * @param currentAssignment Current segment assignment of the table (map from segment name to instance state map)
   * @param instancePartitionsMap Map from type (OFFLINE|CONSUMING|COMPLETED) to instance partitions
   * @param sortedTiers List of Tiers sorted as per priority
   * @param tierInstancePartitionsMap Map from tierName to instance partitions
   * @param config Configuration for the rebalance
   * @return Rebalanced assignment for the segments
   */
  Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config);
}
