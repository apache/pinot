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
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code ImmutableDataSource} class is the data source for a column in the immutable segment.
 */
public class ImmutableDataSource extends BaseDataSource {

  public ImmutableDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    super(new ImmutableDataSourceMetadata(columnMetadata), columnIndexContainer);
  }

  private static class ImmutableDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final boolean _sorted;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final Comparable _minValue;
    final Comparable _maxValue;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;

    ImmutableDataSourceMetadata(ColumnMetadata columnMetadata) {
      _fieldSpec = columnMetadata.getFieldSpec();
      _sorted = columnMetadata.isSorted();
      _numDocs = columnMetadata.getTotalDocs();
      _numValues = columnMetadata.getTotalNumberOfEntries();
      if (_fieldSpec.isSingleValueField()) {
        _maxNumValuesPerMVEntry = -1;
      } else {
        _maxNumValuesPerMVEntry = columnMetadata.getMaxNumberOfMultiValues();
      }
      _minValue = columnMetadata.getMinValue();
      _maxValue = columnMetadata.getMaxValue();
      _partitionFunction = columnMetadata.getPartitionFunction();
      _partitions = columnMetadata.getPartitions();
      _cardinality = columnMetadata.getCardinality();
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return _sorted;
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

    @Override
    public int getCardinality() {
      return _cardinality;
    }
  }
}
