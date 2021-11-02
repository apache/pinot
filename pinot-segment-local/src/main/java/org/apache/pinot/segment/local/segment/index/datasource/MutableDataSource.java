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
package org.apache.pinot.segment.local.segment.index.datasource;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code MutableDataSource} class is the data source for a column in the mutable segment.
 */
@SuppressWarnings("rawtypes")
public class MutableDataSource extends BaseDataSource {
  private final boolean _enableFST;
  private final boolean _enableNativeFST;

  public MutableDataSource(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
      @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions, @Nullable Comparable minValue,
      @Nullable Comparable maxValue, ForwardIndexReader forwardIndex, @Nullable Dictionary dictionary,
      @Nullable InvertedIndexReader invertedIndex, @Nullable RangeIndexReader rangeIndex,
      @Nullable TextIndexReader textIndex, boolean enableFST, boolean enableNativeFST,
      @Nullable JsonIndexReader jsonIndex,
      @Nullable H3IndexReader h3Index, @Nullable BloomFilterReader bloomFilter,
      @Nullable NullValueVectorReader nullValueVector) {
    super(new MutableDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, partitionFunction,
            partitions, minValue, maxValue), forwardIndex, dictionary, invertedIndex, rangeIndex, textIndex, null,
        null, jsonIndex, h3Index, bloomFilter, nullValueVector);
    _enableFST = enableFST;
    _enableNativeFST = enableNativeFST;
  }

  public boolean isFSTEnabled() {
    return _enableFST;
  }

  public boolean isNativeFSTEnabled() {
    return _enableNativeFST;
  }

  private static class MutableDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;

    MutableDataSourceMetadata(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
        @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions, @Nullable Comparable minValue,
        @Nullable Comparable maxValue) {
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

    @Nullable
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
  }
}
