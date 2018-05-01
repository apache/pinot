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
package com.linkedin.pinot.controller.helix.core.realtime.segment;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The default flush threshold updation strategy, which computes the flush threshold size of the segment
 * by dividing the flush threshold of the table by the max number of partitions consuming on an instance
 */
public class DefaultFlushThresholdUpdater extends FlushThresholdUpdater {

  public DefaultFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    super(realtimeTableConfig);
  }

  @Override
  public void updateFlushThreshold(LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      FlushThresholdUpdaterParams params) {

    int tableFlushSize = getRealtimeTableFlushSizeForTable(_realtimeTableConfig);

    // If config does not have a flush threshold, use the default.
    if (tableFlushSize < 1) {
      tableFlushSize = KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount();
    }

    // Gather list of instances for this partition
    PartitionAssignment partitionAssignment = params.getPartitionAssignment();
    String partitionId = new LLCSegmentName(newSegmentZKMetadata.getSegmentName()).getPartitionRange();
    List<String> instancesListForPartition = partitionAssignment.getInstancesListForPartition(partitionId);
    Map<String, Integer> partitionCountForInstance = new HashMap<>(instancesListForPartition.size());
    instancesListForPartition.forEach(instance -> partitionCountForInstance.put(instance, 0));

    // Find partition count for each instance
    int maxPartitionCountPerInstance = 1;
    for (Map.Entry<String, List<String>> partitionAndInstanceList : partitionAssignment.getPartitionToInstances().entrySet()) {
      List<String> instances = partitionAndInstanceList.getValue();
      for (String instance : instances) {
        if (partitionCountForInstance.containsKey(instance)) {
          int partitionCount = partitionCountForInstance.get(instance) + 1;
          partitionCountForInstance.put(instance, partitionCount);
          if (maxPartitionCountPerInstance < partitionCount) {
            maxPartitionCountPerInstance = partitionCount;
          }
        }
      }
    }

    // Configure the segment size flush limit based on the maximum number of partitions allocated to a replica
    int segmentFlushSize = (int) (((float) tableFlushSize) / maxPartitionCountPerInstance);
    newSegmentZKMetadata.setSizeThresholdToFlushSegment(segmentFlushSize);
  }

}
