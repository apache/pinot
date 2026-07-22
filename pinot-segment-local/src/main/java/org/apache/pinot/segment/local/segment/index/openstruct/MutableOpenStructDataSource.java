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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/// Per-key {@link DataSource} accessor for mutable (consuming) segments with an OPEN_STRUCT column.
///
/// Always columnar — no blob branch. Per-key DataSources are synthesized on demand from the
/// underlying {@link MutableOpenStructIndex}; mutable mode always holds every observed key, so
/// {@link #isFullyMaterialized()} is unconditionally {@code true}.
public class MutableOpenStructDataSource extends BaseDataSource implements OpenStructDataSource {
  private final ComplexFieldSpec _fieldSpec;
  private final MutableOpenStructIndex _index;
  private final int _numDocs;

  public MutableOpenStructDataSource(ComplexFieldSpec fieldSpec, MutableOpenStructIndex index, int numDocs) {
    super(new MutableOpenStructDataSourceMetadata(fieldSpec, numDocs),
        new ColumnIndexContainer.FromMap.Builder().build());
    _fieldSpec = fieldSpec;
    _index = index;
    _numDocs = numDocs;
  }

  @Override
  public ComplexFieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  @Nullable
  public DataSource getDataSource(String key) {
    Map<IndexType, IndexReader> indexes = _index.getIndexes(key);
    if (indexes == null || indexes.isEmpty()) {
      return null;
    }
    ColumnMetadata metadata = _index.getColumnMetadata(key);
    return new ImmutableDataSource(metadata,
        new ColumnIndexContainer.FromMap.Builder().withAll(indexes).build());
  }

  @Override
  public boolean isMaterialized(String key) {
    return _index.getKeyColumn(key) != null;
  }

  /// Mutable mode always holds every observed key in-memory; the sparse tier exists only after seal.
  @Override
  public boolean isFullyMaterialized() {
    return true;
  }

  @Override
  public Map<String, DataSource> getDataSources() {
    Map<String, DataSource> result = new HashMap<>();
    for (String key : _index.getKeys()) {
      DataSource ds = getDataSource(key);
      if (ds != null) {
        result.put(key, ds);
      }
    }
    return result;
  }

  @Override
  @Nullable
  public DataSourceMetadata getDataSourceMetadata(String key) {
    DataSource ds = getDataSource(key);
    return ds != null ? ds.getDataSourceMetadata() : null;
  }

  @Override
  @Nullable
  public ColumnIndexContainer getIndexContainer(String key) {
    DataSource ds = getDataSource(key);
    return ds instanceof ImmutableDataSource imm ? imm.getIndexContainer() : null;
  }

  @Override
  @Nullable
  public Map<String, Object> getMapValue(int docId) {
    return _index.getMapValue(docId);
  }

  private static class MutableOpenStructDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    MutableOpenStructDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
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
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return 0;
    }

    @Override
    public int getCardinality() {
      return Constants.UNKNOWN_CARDINALITY;
    }

    @Override
    @Nullable
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Override
    @Nullable
    public java.util.Set<Integer> getPartitions() {
      return null;
    }

    @Override
    @Nullable
    public Comparable<?> getMinValue() {
      return null;
    }

    @Override
    @Nullable
    public Comparable<?> getMaxValue() {
      return null;
    }
  }
}
