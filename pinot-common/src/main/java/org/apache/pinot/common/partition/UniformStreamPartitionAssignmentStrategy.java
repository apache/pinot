package org.apache.pinot.common.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * Uniform partition assignment strategy implementation which uniformly sprays partitions across available hosts
 */
public class UniformStreamPartitionAssignmentStrategy implements StreamPartitionAssignmentStrategy {

  /**
   * Uniformly sprays the partitions and replicas across given list of instances
   * Picks starting point based on table hash value. This ensures that we will always pick the same starting point,
   * and return consistent assignment across calls
   */
  @Override
  public PartitionAssignment getStreamPartitionAssignment(HelixManager helixManager, @Nonnull String tableNameWithType,
      @Nonnull List<String> partitions, int numReplicas, List<String> allTaggedInstances)
      throws InvalidConfigException {

    if (allTaggedInstances.size() < numReplicas) {
      throw new InvalidConfigException(
          "Not enough consuming instances tagged for UniformStreamPartitionAssignment. Must be at least equal to numReplicas:"
              + numReplicas);
    }

    PartitionAssignment partitionAssignment = new PartitionAssignment(tableNameWithType);

    Collections.sort(allTaggedInstances);
    int numInstances = allTaggedInstances.size();
    int serverId = Math.abs(EqualityUtils.hashCodeOf(tableNameWithType)) % numInstances;
    for (String partition : partitions) {
      List<String> instances = new ArrayList<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instances.add(allTaggedInstances.get(serverId));
        serverId = (serverId + 1) % numInstances;
      }
      partitionAssignment.addPartition(partition, instances);
    }
    return partitionAssignment;
  }
}
