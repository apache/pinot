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

import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType;


/**
 * Factory class for constructing the right {@link StreamPartitionAssignmentStrategy} from the table config
 */
public class StreamPartitionAssignmentStrategyFactory {

  /**
   * Given a table config, get the {@link StreamPartitionAssignmentStrategy}
   */
  static StreamPartitionAssignmentStrategy getStreamPartitionAssignmentStrategy(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (validationConfig != null) {
      String segmentAssignmentStrategy = validationConfig.getSegmentAssignmentStrategy();
      if (segmentAssignmentStrategy != null
          && SegmentAssignmentStrategyType.ReplicaGroupSegmentAssignmentStrategy.toString()
          .equalsIgnoreCase(segmentAssignmentStrategy)) {
        return new ReplicaGroupBasedStreamPartitionAssignmentStrategy();
      }
    }
    return new UniformStreamPartitionAssignmentStrategy();
  }
}
