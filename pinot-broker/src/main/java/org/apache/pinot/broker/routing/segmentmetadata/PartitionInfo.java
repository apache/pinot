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
package org.apache.pinot.broker.routing.segmentmetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;


public class PartitionInfo {
  public static final PartitionInfo INVALID_PARTITION_INFO = new PartitionInfo(null, null);
  public final PartitionFunction _partitionFunction;
  public final Set<Integer> _partitions;
  private final int _hashCode;

  public PartitionInfo(PartitionFunction partitionFunction, Set<Integer> partitions) {
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _hashCode = Objects.hash(_partitionFunction, _partitions);
  }

  public Set<Integer> getPartitions() {
    return _partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (hashCode() != o.hashCode()) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) o;
    if (_partitions.size() != that._partitions.size()) {
      return false;
    }
    if (!(_partitionFunction.getFunctionType()
      .equals(that._partitionFunction.getFunctionType())
      && _partitionFunction.getNumPartitions() == that._partitionFunction.getNumPartitions())) {
      return false;
    }
    if (_partitions.size() == 1) {
      if (_partitions.iterator().next().equals(that._partitions.iterator().next())) {
        return true;
      }
    }
    return _partitions.equals(that._partitions);
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }

  @Nullable
  public static Set<String> getPartitionColumnFromConfig(TableConfig tableConfig) {
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig == null) {
      return null;
    }
    Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
    if (columnPartitionMap == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(columnPartitionMap.keySet());
  }
}
