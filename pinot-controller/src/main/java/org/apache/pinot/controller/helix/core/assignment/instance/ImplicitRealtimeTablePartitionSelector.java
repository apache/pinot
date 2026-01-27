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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/**
 * Variation of {@link InstanceReplicaGroupPartitionSelector} that uses the number and IDs of partitions
 * from the stream to determine the partitions in each replica group. When the stream exposes partition IDs
 * (e.g. Kafka with subset), instance partitions are keyed by those IDs so non-contiguous subsets work.
 */
public class ImplicitRealtimeTablePartitionSelector extends InstanceReplicaGroupPartitionSelector {
  private final int _numPartitions;
  private final List<Integer> _partitionIds;

  public ImplicitRealtimeTablePartitionSelector(TableConfig tableConfig,
      InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig, String tableNameWithType,
      @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement) {
    this(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions, minimizeDataMovement,
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
    try (streamMetadataProvider) {
      Set<Integer> ids = streamMetadataProvider.fetchPartitionIds(10_000L);
      if (ids != null && !ids.isEmpty()) {
        _partitionIds = new ArrayList<>(ids);
        Collections.sort(_partitionIds);
        _numPartitions = _partitionIds.size();
      } else {
        _numPartitions = streamMetadataProvider.fetchPartitionCount(10_000L);
        _partitionIds = null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve partition info for table: " + _tableNameWithType, e);
    }
  }

  @Override
  protected int getNumPartitions() {
    return _numPartitions;
  }

  @Nullable
  @Override
  protected List<Integer> getPartitionIds() {
    return _partitionIds;
  }

  @Override
  protected int getNumInstancesPerPartition(int numInstancesPerReplicaGroup) {
    // This partition selector should only be used for CONSUMING instance partitions, and we enforce a single instance
    // per partition in this case.
    return 1;
  }
}
