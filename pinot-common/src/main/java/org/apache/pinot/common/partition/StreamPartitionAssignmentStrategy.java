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
  PartitionAssignment getStreamPartitionAssignment(HelixManager helixManager, @Nonnull String tableNameWithType,
      @Nonnull List<String> partitions, int numReplicas, List<String> instances) throws InvalidConfigException;
}
