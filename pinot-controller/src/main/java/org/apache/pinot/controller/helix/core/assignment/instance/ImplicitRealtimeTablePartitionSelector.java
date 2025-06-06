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
package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/**
 * Variation of {@link InstanceReplicaGroupPartitionSelector} that uses the number of partitions from the stream
 * to determine the number of partitions in each replica group.
 */
public class ImplicitRealtimeTablePartitionSelector extends InstanceReplicaGroupPartitionSelector {
  private final int _numPartitions;

  public ImplicitRealtimeTablePartitionSelector(TableConfig tableConfig,
      InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig, String tableNameWithType,
      @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement) {
    this(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions, minimizeDataMovement,
        // Get the number of partitions from the first stream config
        // TODO: Revisit this logic to better handle multiple streams in the future - either validate that they
        //       all have the same number of partitions and use that or disallow the use of this selector in case the
        //       partition counts differ.
        StreamConsumerFactoryProvider.create(IngestionConfigUtils.getFirstStreamConfig(tableConfig))
            .createStreamMetadataProvider(
                ImplicitRealtimeTablePartitionSelector.class.getSimpleName() + "-" + tableNameWithType)
    );
  }

  @VisibleForTesting
  ImplicitRealtimeTablePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement,
      StreamMetadataProvider streamMetadataProvider) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions, minimizeDataMovement);
    _numPartitions = getStreamNumPartitions(streamMetadataProvider);
  }

  private int getStreamNumPartitions(StreamMetadataProvider streamMetadataProvider) {
    try (streamMetadataProvider) {
      return streamMetadataProvider.fetchPartitionCount(10_000L);
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve partition info for table: " + _tableNameWithType, e);
    }
  }

  @Override
  protected int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  protected int getNumInstancesPerPartition(int numInstancesPerReplicaGroup) {
    // This partition selector should only be used for CONSUMING instance partitions, and we enforce a single instance
    // per partition in this case.
    return 1;
  }
}
