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
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to represent the segment assignment strategy based on the concept of a replica group.
 *
 * A replica group is a pool of servers that is guaranteed to have all segments for a partition or a table
 * (depend on the configuration). If the broker is aware of the replica group, the broker can prune servers
 * when scattering and gathering requests because queries can be answered with a subset of servers.
 *
 */
public class ReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupSegmentAssignmentStrategy.class);
  private static final Random random = new Random(System.currentTimeMillis());

  @Override
  public List<String> getAssignedInstances(HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore,
      String helixClusterName, SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {

    // Parse information from the input metadata.
    SegmentMetadataImpl metadata = (SegmentMetadataImpl) segmentMetadata;
    String tableName = segmentMetadata.getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);

    // Fetch the partition mapping table from the property store.
    PartitionToReplicaGroupMappingZKMetadata partitionToReplicaGroupMapping =
        ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(propertyStore, tableName);

    // Fetch the segment assignment related configurations.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, offlineTableName);
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    boolean mirrorAssignmentAcrossReplicaGroups = replicaGroupStrategyConfig.getMirrorAssignmentAcrossReplicaGroups();

    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();

    int partitionNumber = 0;
    if (partitionColumn != null) {
      // TODO: Need to address when we have multiple partition numbers.
      partitionNumber = metadata.getColumnMetadataFor(replicaGroupStrategyConfig.getPartitionColumn())
          .getPartitionRanges()
          .get(0)
          .getMaximumInteger();
    }

    // Perform the segment assignment.
    // If mirror assignment is on, we randomly pick the index and use the same index for all replica groups.
    // Else, we randomly pick server from each replica group.
    List<String> selectedInstanceList = new ArrayList<>();
    int index = 0;
    for (int groupId = 0; groupId < numReplicas; groupId++) {
      List<String> instancesInReplicaGroup =
          partitionToReplicaGroupMapping.getInstancesfromReplicaGroup(partitionNumber, groupId);
      int numInstances = instancesInReplicaGroup.size();
      if (mirrorAssignmentAcrossReplicaGroups) {
        // Randomly pick the index and use the same index for all replica groups.
        if (groupId == 0) {
          index = random.nextInt(numInstances);
        }
      } else {
        // Randomly pick the index for all replica groups.
        index = random.nextInt(numInstances);
      }
      selectedInstanceList.add(instancesInReplicaGroup.get(index));
    }

    LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
        + segmentMetadata.getTableName() + ", selected instances: " + Arrays.toString(selectedInstanceList.toArray()));

    return selectedInstanceList;
  }
}
