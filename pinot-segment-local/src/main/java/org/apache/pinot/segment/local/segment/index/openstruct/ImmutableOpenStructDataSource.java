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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;


/// Per-key {@link DataSource} accessor for sealed OPEN_STRUCT segments. Dense keys get
/// materialized DataSources; sparse keys get virtual [SparseKeyDataSource]s backed by the
/// shared blob parser. Manifest-absent keys return null (definitively absent).
public class ImmutableOpenStructDataSource extends BaseDataSource implements OpenStructDataSource {
  private final ComplexFieldSpec _fieldSpec;
  private final Map<String, DataSource> _perKeyDataSources;
  @Nullable
  private final DataSource _sparseDataSource;
  @Nullable
  private final Set<String> _sparseKeys;
  @Nullable
  private final OpenStructSparseBlobReader _sparseBlobReader;
  private final ConcurrentHashMap<String, DataSource> _sparseKeyDataSourceCache;

  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, DataSourceMetadata dataSourceMetadata,
      ColumnIndexContainer indexContainer, @Nullable List<String> sparseKeys) {
    super(dataSourceMetadata, indexContainer);
    _fieldSpec = fieldSpec;
    _perKeyDataSources = perKeyDataSources;
    _sparseDataSource = sparseDataSource;
    _sparseKeys = sparseKeys != null ? Set.copyOf(sparseKeys) : null;
    if (sparseDataSource != null) {
      ForwardIndexReader<?> blobFwd = sparseDataSource.getForwardIndex();
      _sparseBlobReader = blobFwd != null
          ? new OpenStructSparseBlobReader(blobFwd, sparseDataSource.getNullValueVector(),
              dataSourceMetadata.getNumDocs())
          : null;
    } else {
      _sparseBlobReader = null;
    }
    _sparseKeyDataSourceCache = new ConcurrentHashMap<>();
  }

  /// Convenience constructor for segment-load time. Synthesizes a minimal [DataSourceMetadata]
  /// for the parent OPEN_STRUCT column (which has no on-disk presence of its own) and uses an empty
  /// [ColumnIndexContainer] — all real readers live on the per-key data sources.
  ///
  /// The parent's `getForwardIndex()` / `getDictionary()` will return `null`.
  /// Callers must use [#getDataSource(String)] for per-key access; whole-struct projection
  /// (`SELECT open_struct_col`) is handled by the query layer, not the storage layer.
  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, int numDocs, @Nullable List<String> sparseKeys) {
    this(fieldSpec, perKeyDataSources, sparseDataSource,
        new ImmutableOpenStructDataSourceMetadata(fieldSpec, numDocs),
        new ColumnIndexContainer.FromMap.Builder().build(), sparseKeys);
  }

  @Override
  public ComplexFieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  @Nullable
  public DataSource getDataSource(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    if (ds != null) {
      return ds;
    }
    if (_sparseBlobReader == null) {
      return null;
    }
    if (_sparseKeys != null && !_sparseKeys.contains(key)) {
      return null;
    }
    return _sparseKeyDataSourceCache.computeIfAbsent(key, this::buildSparseKeyDataSource);
  }

  private DataSource buildSparseKeyDataSource(String key) {
    FieldSpec childSpec = _fieldSpec.getChildFieldSpec(key);
    if (childSpec == null) {
      childSpec = new DimensionFieldSpec(key, FieldSpec.DataType.STRING, true);
    }
    return new SparseKeyDataSource(childSpec, _sparseBlobReader);
  }

  @Override
  public boolean isMaterialized(String key) {
    return _perKeyDataSources.containsKey(key);
  }

  @Override
  public boolean isFullyMaterialized() {
    return _sparseDataSource == null;
  }

  /// Returns only the materialized (dense) key DataSources. Sparse keys are not included because
  /// they share a single JSON column and have no individual materialized DataSource.
  @Override
  public Map<String, DataSource> getDataSources() {
    return _perKeyDataSources;
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
    return ds instanceof ImmutableDataSource immutableDs ? immutableDs.getIndexContainer() : null;
  }

  @Override
  @Nullable
  public JsonIndexReader getSparseJsonIndex() {
    return _sparseDataSource != null ? _sparseDataSource.getJsonIndex() : null;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public Map<String, Object> getMapValue(int docId) {
    Map<String, Object> result = null;

    for (Map.Entry<String, DataSource> entry : _perKeyDataSources.entrySet()) {
      Object value = readValue(entry.getKey(), entry.getValue(), docId);
      if (value != null) {
        if (result == null) {
          result = new HashMap<>();
        }
        result.put(entry.getKey(), value);
      }
    }

    if (_sparseDataSource != null) {
      Object sparseValue = readValue(_fieldSpec.getName(), _sparseDataSource, docId);
      if (sparseValue instanceof String) {
        String json = (String) sparseValue;
        if (!json.isEmpty()) {
          try {
            Map<String, Object> sparseMap = JsonUtils.stringToObject(json, Map.class);
            if (result == null) {
              result = new HashMap<>();
            }
            result.putAll(sparseMap);
          } catch (IOException e) {
            throw new RuntimeException("Failed to parse sparse JSON at docId " + docId, e);
          }
        }
      }
    }

    return result;
  }

  /// Reads the value of `key` at `docId`, or `null` when the doc is null or the column has no
  /// forward index. Delegates the null-vector check and the dictionary/raw per-type read dispatch to
  /// [PinotSegmentColumnReader] rather than re-deriving them here, so this path cannot drift from the
  /// reader every other column read in the engine already goes through. OPEN_STRUCT child columns are
  /// always single-valued, hence the 0 maxNumValuesPerMVEntry.
  @Nullable
  private static Object readValue(String key, DataSource dataSource, int docId) {
    ForwardIndexReader<?> fwdReader = dataSource.getForwardIndex();
    if (fwdReader == null) {
      return null;
    }
    try (PinotSegmentColumnReader reader = new PinotSegmentColumnReader(key, fwdReader, dataSource.getDictionary(),
        dataSource.getNullValueVector(), 0)) {
      return reader.isNull(docId) ? null : reader.getValue(docId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read value from OPEN_STRUCT key forward index", e);
    }
  }

  private static class ImmutableOpenStructDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    ImmutableOpenStructDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
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
      return null;
    }

    @Nullable
    @Override
    public java.util.Set<Integer> getPartitions() {
      return null;
    }
  }
}
