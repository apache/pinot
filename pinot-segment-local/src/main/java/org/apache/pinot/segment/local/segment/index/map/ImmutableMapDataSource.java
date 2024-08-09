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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code MutableDataSource} class is the data source for a map type column in the mutable segment.
 */
@SuppressWarnings("rawtypes")
public class ImmutableMapDataSource extends MapDataSource {
  public ImmutableMapDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    super(new ImmutableMapDataSourceMetadata(columnMetadata), columnIndexContainer);
  }

  public DataSource getKeyDataSource(String key) {
    MapIndexReader mir = (MapIndexReader) getForwardIndex();
    assert mir != null;
    Map<IndexType, IndexReader> indexes = mir.getKeyIndexes(key);

    if (indexes == null) {
      // The key does not exist in the map
      return new NullDataSource(key);
    }

    try (ColumnIndexContainer indexContainer = new ColumnIndexContainer.FromMap(indexes)) {
      ColumnMetadata keyMeta = mir.getKeyMetadata(key);
      return new ImmutableDataSource(keyMeta, indexContainer);
    } catch (Exception ex) {
      // TODO(ERICH): what to do here?
    }

    return null;
  }

  private static class ImmutableMapDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;

    ImmutableMapDataSourceMetadata(ColumnMetadata columnMetadata) {
      _fieldSpec = columnMetadata.getFieldSpec();
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
      throw new UnsupportedOperationException();
    }
  }
}
