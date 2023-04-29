package org.apache.pinot.controller.helix.core.assignment.utils;

import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


public class SegmentUtils {
  private SegmentUtils() {
  }

  public static String getPartitionColumn(TableConfig tableConfig) {
    String partitionColumn;
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      InstanceAssignmentConfig instanceAssignmentConfig =
          InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE);

      partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
    } else {
      InstanceAssignmentConfig instanceAssignmentConfig =
          InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
      partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
      if (partitionColumn == null) {
        instanceAssignmentConfig = InstanceAssignmentConfigUtils.
            getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
        partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
      }
    }
    return partitionColumn;
  }
}
