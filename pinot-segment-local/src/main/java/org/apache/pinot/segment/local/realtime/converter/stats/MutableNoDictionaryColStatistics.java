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
package org.apache.pinot.segment.local.realtime.converter.stats;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndex;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.partition.PartitionFunction;

import static org.apache.pinot.segment.spi.Constants.UNKNOWN_CARDINALITY;


public class MutableNoDictionaryColStatistics implements ColumnStatistics, CLPStatsProvider {
  private final DataSourceMetadata _dataSourceMetadata;
  private final MutableForwardIndex _forwardIndex;

  public MutableNoDictionaryColStatistics(DataSource dataSource) {
    _dataSourceMetadata = dataSource.getDataSourceMetadata();
    _forwardIndex = (MutableForwardIndex) dataSource.getForwardIndex();
    Preconditions.checkState(_forwardIndex != null,
        String.format("Forward index should not be null for column: %s", _dataSourceMetadata.getFieldSpec().getName()));
  }

  @Override
  public Object getMinValue() {
    return _dataSourceMetadata.getMinValue();
  }

  @Override
  public Object getMaxValue() {
    return _dataSourceMetadata.getMaxValue();
  }

  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return UNKNOWN_CARDINALITY;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _forwardIndex.getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLargestElement() {
    return _forwardIndex.getLengthOfLongestElement();
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _dataSourceMetadata.getNumDocs();
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _dataSourceMetadata.getMaxNumValuesPerMVEntry();
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _dataSourceMetadata.getPartitionFunction();
  }

  @Override
  public int getNumPartitions() {
    PartitionFunction partitionFunction = _dataSourceMetadata.getPartitionFunction();
    if (partitionFunction != null) {
      return partitionFunction.getNumPartitions();
    } else {
      return 0;
    }
  }

  @Override
  public Map<String, String> getPartitionFunctionConfig() {
    PartitionFunction partitionFunction = _dataSourceMetadata.getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getFunctionConfig() : null;
  }

  @Override
  public Set<Integer> getPartitions() {
    return _dataSourceMetadata.getPartitions();
  }

  @Override
  public CLPStats getCLPStats() {
    if (_forwardIndex instanceof CLPMutableForwardIndex) {
      return ((CLPMutableForwardIndex) _forwardIndex).getCLPStats();
    }
    throw new IllegalStateException(
        "CLP stats not available for column: " + _dataSourceMetadata.getFieldSpec().getName());
  }
}
