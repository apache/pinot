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
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


@SuppressWarnings("rawtypes")
public class ImmutableMapDataSource extends BaseMapDataSource {
  private final MapIndexReader _mapIndexReader;

  public ImmutableMapDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    super(new ImmutableMapDataSourceMetadata(columnMetadata), columnIndexContainer);
    MapIndexReader mapIndexReader;
    ForwardIndexReader<?> forwardIndex = getForwardIndex();
    if (forwardIndex instanceof MapIndexReader) {
      mapIndexReader = (MapIndexReader) forwardIndex;
    } else {
      mapIndexReader = new MapIndexReaderWrapper(forwardIndex, getFieldSpec(), columnMetadata.getTotalDocs());
    }
    _mapIndexReader = mapIndexReader;
  }

  @Override
  public MapIndexReader<ForwardIndexReaderContext> getMapIndexReader() {
    return _mapIndexReader;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String key) {
    return null;
  }

  @Override
  public ColumnIndexContainer getIndexContainer(String key) {
    return null;
  }

  /// Exposes the MAP column's [ColumnMetadata] through the [DataSourceMetadata] view by delegating every accessor
  /// through a single reference instead of copying the fields (see the equivalent adapter in `ImmutableDataSource`).
  /// A MAP column is never reported as sorted and does not support the max row length.
  private static class ImmutableMapDataSourceMetadata implements DataSourceMetadata {
    final ColumnMetadata _columnMetadata;

    ImmutableMapDataSourceMetadata(ColumnMetadata columnMetadata) {
      _columnMetadata = columnMetadata;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _columnMetadata.getFieldSpec();
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _columnMetadata.getTotalDocs();
    }

    @Override
    public int getNumValues() {
      return _columnMetadata.getTotalNumberOfEntries();
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      // DataSourceMetadata reports -1 for single-value columns, whereas ColumnMetadata reports 0
      return _columnMetadata.getFieldSpec().isSingleValueField() ? -1 : _columnMetadata.getMaxNumberOfMultiValues();
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return _columnMetadata.getMinValue();
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return _columnMetadata.getMaxValue();
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return _columnMetadata.getPartitionFunction();
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return _columnMetadata.getPartitions();
    }

    @Override
    public int getCardinality() {
      return _columnMetadata.getCardinality();
    }

    @Override
    public int getMaxRowLengthInBytes() {
      throw new UnsupportedOperationException();
    }
  }
}
