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
