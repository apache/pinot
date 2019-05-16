package org.apache.pinot.common.partition;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.InvalidConfigException;


/**
 * Replica group based partition assignment strategy for realtime partitions
 */
public class ReplicaGroupBasedStreamPartitionAssignmentStrategy implements StreamPartitionAssignmentStrategy {

  /**
   * Fetches the replica group partition assignment znode and assigns partitions across the replica groups.
   * A vertical slice will be picked form the replica group sets for a partition, based on the formula:
   * vertical slice = partition % numInstancesPerReplicaGroup
   */
  @Override
  public PartitionAssignment getStreamPartitionAssignment(HelixManager helixManager, @Nonnull String tableNameWithType,
      @Nonnull List<String> partitions, int numReplicas, List<String> instances) throws InvalidConfigException {

    ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment =
        getReplicaGroupPartitionAssignment(helixManager, tableNameWithType);
    if (replicaGroupPartitionAssignment == null) {
      throw new InvalidConfigException("ReplicaGroupPartitionAssignment is null for table:" + tableNameWithType);
    }
    int numReplicaGroups = replicaGroupPartitionAssignment.getNumReplicaGroups();
    if (numReplicaGroups != numReplicas) {
      throw new InvalidConfigException(
          "numReplicas:" + numReplicas + " is not equal to numReplicaGroups:" + numReplicaGroups
              + " from znode for table:" + tableNameWithType);
    }
    int numInstancesPerReplicaGroup = replicaGroupPartitionAssignment.getInstancesFromReplicaGroup(0, 0).size();

    PartitionAssignment streamPartitionAssignment = new PartitionAssignment(tableNameWithType);

    List<List<String>> verticalSlices = new ArrayList<>(numInstancesPerReplicaGroup);
    for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
      verticalSlices.add(new ArrayList<>(numReplicas));
    }

    for (int replicaGroupNumber = 0; replicaGroupNumber < numReplicas; replicaGroupNumber++) {
      List<String> instancesFromReplicaGroup =
          replicaGroupPartitionAssignment.getInstancesFromReplicaGroup(0, replicaGroupNumber);
      for (int serverIndex = 0; serverIndex < numInstancesPerReplicaGroup; serverIndex++) {
        verticalSlices.get(serverIndex).add(instancesFromReplicaGroup.get(serverIndex));
      }
    }

    for (String partition : partitions) {
      int verticalSlice = Integer.parseInt(partition) % numInstancesPerReplicaGroup;
      streamPartitionAssignment.addPartition(partition, verticalSlices.get(verticalSlice));
    }
    return streamPartitionAssignment;
  }

  @VisibleForTesting
  protected ReplicaGroupPartitionAssignment getReplicaGroupPartitionAssignment(HelixManager helixManager,
      String tableNameWithType) {
    ReplicaGroupPartitionAssignmentGenerator replicaGroupPartitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(helixManager.getHelixPropertyStore());
    return replicaGroupPartitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
  }
}
