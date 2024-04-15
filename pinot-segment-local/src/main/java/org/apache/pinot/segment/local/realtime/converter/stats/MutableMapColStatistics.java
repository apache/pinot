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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.index.map.MapDataSource;
import org.apache.pinot.segment.spi.creator.MapColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableMapIndex;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;

import static org.apache.pinot.segment.spi.Constants.UNKNOWN_CARDINALITY;


public class MutableMapColStatistics implements MapColumnStatistics {
  private final DataSourceMetadata _dataSourceMetadata;
  private final MutableMapIndex _mapIndex;
  private final MapDataSource _mapDataSource;

  public MutableMapColStatistics(MapDataSource dataSource) {
    _mapDataSource = dataSource;
    _dataSourceMetadata = dataSource.getDataSourceMetadata();
    _mapIndex = (MutableMapIndex) dataSource.getForwardIndex();
    Preconditions.checkState(_mapIndex != null,
        String.format("Forward index should not be null for column: %s", _dataSourceMetadata.getFieldSpec().getName()));
  }

  @Override
  public Set<Pair<String, FieldSpec.DataType>> getKeys() {
    return _mapIndex.getKeys();
  }

  @Override
  public Object getMinValueForKey(String key) {
    return _mapDataSource.getKeyDataSource(key).getDataSourceMetadata().getMinValue();
  }

  @Override
  public Object getMaxValueForKey(String key) {
    return _mapDataSource.getKeyDataSource(key).getDataSourceMetadata().getMaxValue();
  }

  @Override
  public int getLengthOfShortestElementForKey(String key) {
    return ((MutableForwardIndex) (_mapDataSource.getKeyDataSource(key).getForwardIndex()))
        .getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLargestElementForKey(String key) {
    return ((MutableForwardIndex) (_mapDataSource.getKeyDataSource(key).getForwardIndex())).getLengthOfLongestElement();
  }

  @Override
  public boolean isSortedForKey(String key) {
    return _mapDataSource.getKeyDataSource(key).getDataSourceMetadata().isSorted();
  }

  @Override
  public int getTotalNumberOfEntriesForKey(String key) {
    return _mapDataSource.getKeyDataSource(key).getDataSourceMetadata().getNumDocs();
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
    return _mapIndex.getLengthOfShortestElement();
  }

  @Override
  public int getLengthOfLargestElement() {
    return _mapIndex.getLengthOfLongestElement();
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
}
