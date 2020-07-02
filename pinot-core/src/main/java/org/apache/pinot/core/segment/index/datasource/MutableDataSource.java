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
package org.apache.pinot.core.segment.index.datasource;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code MutableDataSource} class is the data source for a column in the mutable segment.
 */
// TODO: Support min/max value for mutable segment
public class MutableDataSource extends BaseDataSource {
  private static final String OPERATOR_NAME_PREFIX = "MutableDataSource:";

  public MutableDataSource(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
      @Nullable PartitionFunction partitionFunction, int partitionId, DataFileReader forwardIndex,
      @Nullable Dictionary dictionary, @Nullable InvertedIndexReader invertedIndex, @Nullable InvertedIndexReader rangeIndex,
      @Nullable BloomFilterReader bloomFilter, @Nullable NullValueVectorReader nullValueVector) {
    super(new MutableDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, partitionFunction,
            partitionId), forwardIndex, dictionary, invertedIndex, rangeIndex, bloomFilter, nullValueVector,
        OPERATOR_NAME_PREFIX + fieldSpec.getName());
  }

  private static class MutableDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;

    MutableDataSourceMetadata(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
        @Nullable PartitionFunction partitionFunction, int partitionId) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
      _numValues = numValues;
      _maxNumValuesPerMVEntry = maxNumValuesPerMVEntry;
      if (partitionFunction != null) {
        _partitionFunction = partitionFunction;
        _partitions = Collections.singleton(partitionId);
      } else {
        _partitionFunction = null;
        _partitions = null;
      }
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
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
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
