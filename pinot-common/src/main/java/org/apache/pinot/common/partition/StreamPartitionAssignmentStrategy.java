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
package org.apache.pinot.common.partition;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.InvalidConfigException;


/**
 * Creates a partition assignment for the partitions of a realtime table
 */
// TODO: Unify the interfaces for {@link org.apache.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy} and {@link StreamPartitionAssignmentStrategy}
public interface StreamPartitionAssignmentStrategy {

  /**
   * Given the list of partitions and replicas, come up with a {@link PartitionAssignment}
   */
  // TODO: pass current partition assignment to add smarts which can minimize shuffle
  PartitionAssignment getStreamPartitionAssignment(HelixManager helixManager, @Nonnull String tableNameWithType,
      @Nonnull List<String> partitions, int numReplicas, List<String> instances) throws InvalidConfigException;
}
