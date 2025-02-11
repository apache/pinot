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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("rawtypes")
public class ImmutableMapDataSource extends BaseMapDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableMapDataSource.class);
  private final MapIndexReader _mapIndexReader;

  public ImmutableMapDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    super(new ImmutableMapDataSourceMetadata(columnMetadata), columnIndexContainer);
    MapIndexReader mapIndexReader = getMapIndex();
    if (mapIndexReader == null) {
      // Fallback to use forward index
      ForwardIndexReader<?> forwardIndex = getForwardIndex();
      if (forwardIndex instanceof MapIndexReader) {
        mapIndexReader = (MapIndexReader) forwardIndex;
      } else {
        mapIndexReader = new MapIndexReaderWrapper(forwardIndex, getFieldSpec());
      }
    }
    _mapIndexReader = mapIndexReader;
  }

  @Override
  public MapIndexReader<ForwardIndexReaderContext, IndexReader> getMapIndexReader() {
    return _mapIndexReader;
  }

  @Override
  public DataSourceMetadata getKeyDataSourceMetadata(String key) {
    return null;
  }

  @Override
  public ColumnIndexContainer getKeyIndexContainer(String key) {
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
