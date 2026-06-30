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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;



/// Per-key {@link DataSource} accessor for sealed (immutable) segments with an OPEN_STRUCT column.
///
/// Always columnar — there is no blob branch. Every key that was dense enough during segment
/// creation gets its own materialized {@link DataSource} (forward index + optional inverted index /
/// dictionary). Keys that did not meet the density threshold are stored in an optional sparse
/// column; the sparse {@link DataSource} is returned for any unmaterialized key lookup.
///
/// Use [#isMaterialized(String)] and [#isFullyMaterialized()] together to choose
/// the query execution path:
/// - Materialized key → fast path via per-key DataSource (inverted/dictionary index available).
/// - Not materialized + not fully materialized → fall back to the sparse DataSource.
/// - Not materialized + fully materialized → key is definitively absent; short-circuit.
///
/// Thread-safety: immutable after construction; safe for concurrent reads.
public class ImmutableOpenStructDataSource extends BaseDataSource implements OpenStructDataSource {
  private final ComplexFieldSpec _fieldSpec;
  private final Map<String, DataSource> _perKeyDataSources;
  @Nullable
  private final DataSource _sparseDataSource;

  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, DataSourceMetadata dataSourceMetadata,
      ColumnIndexContainer indexContainer) {
    super(dataSourceMetadata, indexContainer);
    _fieldSpec = fieldSpec;
    _perKeyDataSources = perKeyDataSources;
    _sparseDataSource = sparseDataSource;
  }

  /// Convenience constructor for segment-load time. Synthesizes a minimal {@link DataSourceMetadata}
  /// for the parent OPEN_STRUCT column (which has no on-disk presence of its own) and uses an empty
  /// {@link ColumnIndexContainer} — all real readers live on the per-key data sources.
  public ImmutableOpenStructDataSource(ComplexFieldSpec fieldSpec, Map<String, DataSource> perKeyDataSources,
      @Nullable DataSource sparseDataSource, int numDocs) {
    this(fieldSpec, perKeyDataSources, sparseDataSource,
        new ImmutableOpenStructDataSourceMetadata(fieldSpec, numDocs),
        new ColumnIndexContainer.FromMap.Builder().build());
  }

  @Override
  public ComplexFieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  @Nullable
  public DataSource getDataSource(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds != null ? ds : _sparseDataSource;
  }

  @Override
  public boolean isMaterialized(String key) {
    return _perKeyDataSources.containsKey(key);
  }

  @Override
  public boolean isFullyMaterialized() {
    return _sparseDataSource == null;
  }

  @Override
  public Map<String, DataSource> getDataSources() {
    return _perKeyDataSources;
  }

  @Override
  @Nullable
  public DataSourceMetadata getDataSourceMetadata(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds != null ? ds.getDataSourceMetadata() : null;
  }

  @Override
  @Nullable
  public ColumnIndexContainer getIndexContainer(String key) {
    DataSource ds = _perKeyDataSources.get(key);
    return ds instanceof ImmutableDataSource immutableDs ? immutableDs.getIndexContainer() : null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  @Nullable
  public Map<String, Object> getMapValue(int docId) {
    Map<String, Object> result = null;

    for (Map.Entry<String, DataSource> entry : _perKeyDataSources.entrySet()) {
      Object value = readValue(entry.getValue(), docId);
      if (value != null) {
        if (result == null) {
          result = new HashMap<>();
        }
        result.put(entry.getKey(), value);
      }
    }

    if (_sparseDataSource != null) {
      Object sparseValue = readValue(_sparseDataSource, docId);
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  private static Object readValue(DataSource dataSource, int docId) {
    NullValueVectorReader nullReader = dataSource.getNullValueVector();
    if (nullReader != null && nullReader.isNull(docId)) {
      return null;
    }
    ForwardIndexReader fwdReader = dataSource.getForwardIndex();
    if (fwdReader == null) {
      return null;
    }
    try (ForwardIndexReaderContext ctx = fwdReader.createContext()) {
      Dictionary dictionary = dataSource.getDictionary();
      if (dictionary != null) {
        int dictId = fwdReader.getDictId(docId, ctx);
        return dictionary.get(dictId);
      }
      DataType storedType = fwdReader.getStoredType();
      switch (storedType) {
        case INT:
          return fwdReader.getInt(docId, ctx);
        case LONG:
          return fwdReader.getLong(docId, ctx);
        case FLOAT:
          return fwdReader.getFloat(docId, ctx);
        case DOUBLE:
          return fwdReader.getDouble(docId, ctx);
        case BIG_DECIMAL:
          return fwdReader.getBigDecimal(docId, ctx);
        case STRING:
          return fwdReader.getString(docId, ctx);
        case BYTES:
          return fwdReader.getBytes(docId, ctx);
        default:
          throw new IllegalStateException("Unsupported stored type for OPEN_STRUCT key: " + storedType);
      }
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
