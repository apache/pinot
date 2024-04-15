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

package org.apache.pinot.segment.local.segment.index.map;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableMapIndex;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;

/**
 * The {@code MutableDataSource} class is the data source for a map type column in the mutable segment.
 */
@SuppressWarnings("rawtypes")
public class MutableMapDataSource extends MapDataSource {
  Map<IndexType, MutableIndex> _mutableIndexes;
  private final Map<String, DataSource> _keyDataSources;

  public MutableMapDataSource(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
      int cardinality, @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
      @Nullable Comparable minValue, @Nullable Comparable maxValue, Map<IndexType, MutableIndex> mutableIndexes,
      MutableDictionary dictionary, int maxRowLengthInBytes) {
    super(new MutableMapDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, cardinality,
            partitionFunction, partitions, minValue, maxValue, maxRowLengthInBytes),
        new ColumnIndexContainer.FromMap.Builder()
            .withAll(mutableIndexes)
            .build());
    _mutableIndexes = mutableIndexes;
    _keyDataSources = new HashMap<>();
  }

  public DataSource getKeyDataSource(String key) {
    if (_keyDataSources.containsKey(key)) {
      return _keyDataSources.get(key);
    }

    MutableMapDataSourceMetadata md = (MutableMapDataSourceMetadata) getDataSourceMetadata();

    MutableMapIndex mpi = (MutableMapIndex) _mutableIndexes.get(StandardIndexes.forward());

    assert mpi != null;
    Map<IndexType, MutableIndex> indexes = mpi.getKeyIndexes(key);
    DataSource keyDatasource = null;

    if (indexes != null) {
      FieldSpec keyFS = mpi.getKeySpec(key);
      Comparable<?> minValue = mpi.getMinValueForKey(key);
      Comparable<?> maxValue = mpi.getMaxValueForKey(key);
      // Get the length of the longest entry
      MutableForwardIndex fwdIdx = (MutableForwardIndex) indexes.get(StandardIndexes.forward());
      int lengthLongestEntry = fwdIdx.getLengthOfLongestElement();

      keyDatasource = new MutableDataSource(
          keyFS,
          // TODO: We need to get KEY level metadata for these values not Map level metadata
          md._numDocs,
          md._numValues,
          md._maxNumValuesPerMVEntry,
          md._cardinality,
          md._partitionFunction,
          md._partitions,
          minValue,
          maxValue,
          indexes,
          null,
          null,
          lengthLongestEntry
      );
    } else {
      // If the key does not exist then resolve to the Integer Null value
      keyDatasource = new NullDataSource(key);
    }
    _keyDataSources.put(key, keyDatasource);

    return keyDatasource;
  }

  private static class MutableMapDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;
    final int _maxRowLengthInBytes;
    final Set<String> _denseKeys;


    MutableMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
        int cardinality, @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
        @Nullable Comparable minValue, @Nullable Comparable maxValue, int maxRowLengthInBytes) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
      _numValues = numValues;
      _maxNumValuesPerMVEntry = maxNumValuesPerMVEntry;
      if (partitionFunction != null) {
        _partitionFunction = partitionFunction;
        _partitions = partitions;
      } else {
        _partitionFunction = null;
        _partitions = null;
      }
      _minValue = minValue;
      _maxValue = maxValue;
      _cardinality = cardinality;
      _maxRowLengthInBytes = maxRowLengthInBytes;
      _denseKeys = new HashSet<>();
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      // NOTE: Mutable data source is never sorted
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numValues;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return _maxNumValuesPerMVEntry;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return _minValue;
    }

    @Override
    public Comparable getMaxValue() {
      return _maxValue;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return _partitionFunction;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return _partitions;
    }

    @Override
    public int getCardinality() {
      return _cardinality;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return _maxRowLengthInBytes;
    }
  }
}
