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


/// The `ImmutableDataSource` class is the data source for a column in the immutable segment.
public class ImmutableDataSource extends BaseDataSource {

  public ImmutableDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    super(new ImmutableDataSourceMetadata(columnMetadata), columnIndexContainer);
  }

  /// The data-source metadata for a column, without a data source. It delegates to [ColumnMetadata] and holds no
  /// index readers, so a caller that needs only the column's statistics can avoid materializing the column.
  public static DataSourceMetadata metadataOf(ColumnMetadata columnMetadata) {
    return new ImmutableDataSourceMetadata(columnMetadata);
  }

  /// Exposes the segment's [ColumnMetadata] through the [DataSourceMetadata] view by delegating every accessor.
  /// Holding a single reference instead of copying the ten fields the view exposes keeps this object at one
  /// reference per column, which matters for wide segments where every loaded column retains one. The delegation
  /// is observationally equivalent to a snapshot because every [ColumnMetadata] handed to an immutable data source
  /// is itself immutable once built.
  private static class ImmutableDataSourceMetadata implements DataSourceMetadata {
    final ColumnMetadata _columnMetadata;

    ImmutableDataSourceMetadata(ColumnMetadata columnMetadata) {
      _columnMetadata = columnMetadata;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _columnMetadata.getFieldSpec();
    }

    @Override
    public boolean isSorted() {
      return _columnMetadata.isSorted();
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

    // getMaxRowLengthInBytes() is deliberately not delegated: DataSourceMetadata defines it as -1 for immutable
    // columns, while ColumnMetadata computes the serialized row length.
  }
}
