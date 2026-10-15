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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.columntransformer.DataTypeColumnTransformer;
import org.apache.pinot.segment.local.columntransformer.NullValueColumnTransformer;
import org.apache.pinot.segment.local.columntransformer.SpecialValueColumnTransformer;
import org.apache.pinot.segment.local.columntransformer.TimeValidationColumnTransformer;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizationResult;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizedColumnInfo;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;


/// Normalizes one single-value source column through the ingestion transforms for both statistics and indexing.
/// Instances are not thread-safe. The source factory owns the reader; closing this wrapper only releases its cache.
final class NormalizedColumnReader implements ColumnReader {
  private final ColumnReader _source;
  @Nullable
  private final ColumnTransformer _dataTypeTransformer;
  private final ColumnTransformer[] _transformers;
  @Nullable
  private final SanitizedColumnInfo _sanitizedColumnInfo;
  private final BitSet _sanitizedDocIds;
  @Nullable
  private final BoundedJsonMapCache _jsonCache;
  private boolean _trackSanitization;
  private int _lastDocId = -1;
  @Nullable
  private Object _lastValue;
  private boolean _lastValueSanitized;

  NormalizedColumnReader(ColumnReader source, TableConfig tableConfig, FieldSpec fieldSpec, Schema schema,
      BitSet sanitizedDocIds) {
    Preconditions.checkArgument(fieldSpec.isSingleValueField(), "Column must be single-value: %s", fieldSpec.getName());
    _source = Preconditions.checkNotNull(source, "Missing source column: %s", fieldSpec.getName());
    _sanitizedDocIds = Preconditions.checkNotNull(sanitizedDocIds);
    DataTypeColumnTransformer dataTypeTransformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, source);
    boolean canonicalJson = fieldSpec.getDataType() == DataType.JSON && source.isCanonicalJson();
    Preconditions.checkArgument(!canonicalJson || source.getValueType() == PinotDataType.STRING,
        "Canonical JSON reader must expose STRING values: %s", fieldSpec.getName());
    _dataTypeTransformer = canonicalJson || dataTypeTransformer.isNoOp() ? null : dataTypeTransformer;
    _jsonCache = fieldSpec.getDataType() == DataType.JSON && _dataTypeTransformer != null
        ? new BoundedJsonMapCache() : null;
    _transformers = List.of(new TimeValidationColumnTransformer(tableConfig, schema, fieldSpec.getName()),
        new SpecialValueColumnTransformer(fieldSpec), new NullValueColumnTransformer(tableConfig, fieldSpec, schema))
        .stream().filter(transformer -> !transformer.isNoOp()).toArray(ColumnTransformer[]::new);
    _sanitizedColumnInfo = SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec);
  }

  void enableSanitizationTracking() {
    _trackSanitization = true;
  }

  void releaseCache() {
    if (_jsonCache != null) {
      _jsonCache.clear();
    }
    _lastDocId = -1;
    _lastValue = null;
    _lastValueSanitized = false;
  }

  @Override
  public String getColumnName() {
    return _source.getColumnName();
  }

  @Nullable
  @Override
  public PinotDataType getValueType() {
    // Normalization returns internal values, including LONG for logical TIMESTAMP; use the object path.
    return null;
  }

  @Override
  public int getTotalDocs() {
    return _source.getTotalDocs();
  }

  @Override
  public void rewind()
      throws IOException {
    _lastDocId = -1;
    _lastValue = null;
    _lastValueSanitized = false;
    _source.rewind();
  }

  @Override
  public boolean isNull(int docId)
      throws IOException {
    return getValue(docId) == null;
  }

  @Override
  public Object getValue(int docId)
      throws IOException {
    if (_lastDocId != docId || _lastValue == null) {
      Object value = _source.getValue(docId);
      if (_dataTypeTransformer != null) {
        value = _jsonCache == null ? _dataTypeTransformer.transform(value)
            : _jsonCache.transform(value, _dataTypeTransformer);
      }
      for (ColumnTransformer transformer : _transformers) {
        value = transformer.transform(value);
      }
      boolean sanitized = false;
      if (_sanitizedColumnInfo != null) {
        SanitizationResult result = SanitizationTransformerUtils.sanitizeValue(_sanitizedColumnInfo, value);
        if (result != null) {
          value = result.getValue();
          sanitized = result.isSanitized();
        }
      }
      if (value == null) {
        throw new IllegalStateException("Normalization returned null for column: " + getColumnName());
      }
      _lastValue = value;
      _lastValueSanitized = sanitized;
      _lastDocId = docId;
    }
    if (_trackSanitization && _lastValueSanitized) {
      _sanitizedDocIds.set(docId);
    }
    return _lastValue;
  }

  @Override
  public int getInt(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimal(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MultiValueResult<int[]> getIntMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MultiValueResult<long[]> getLongMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MultiValueResult<float[]> getFloatMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MultiValueResult<double[]> getDoubleMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] getBigDecimalMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getStringMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] getBytesMV(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    releaseCache();
  }

  /// Bounds retained JSON conversion results per column, owning immutable keys instead of the reader's mutable maps.
  private static final class BoundedJsonMapCache {
    private static final int CAPACITY = 4096;
    private static final long MAX_RETAINED_WEIGHT = 4L * 1024 * 1024;
    private static final long MAX_ENTRY_WEIGHT = 256L * 1024;

    private final LinkedHashMap<Map<String, String>, CachedJson> _entries =
        new LinkedHashMap<>(CAPACITY, 0.75f, true);
    private long _retainedWeight;

    Object transform(@Nullable Object value, ColumnTransformer transformer) {
      if (!(value instanceof Map<?, ?> map)) {
        return transformer.transform(value);
      }
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
          return transformer.transform(value);
        }
      }
      CachedJson cached = _entries.get(map);
      if (cached != null) {
        return cached.canonical();
      }
      Object normalized = transformer.transform(value);
      if (!(normalized instanceof String canonical)) {
        return normalized;
      }
      long weight = estimatedWeight(map, canonical);
      if (weight > MAX_ENTRY_WEIGHT) {
        return canonical;
      }
      Map<String, String> snapshot = new HashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        snapshot.put((String) entry.getKey(), (String) entry.getValue());
      }
      _entries.put(Map.copyOf(snapshot), new CachedJson(canonical, weight));
      _retainedWeight += weight;
      while (_entries.size() > CAPACITY || _retainedWeight > MAX_RETAINED_WEIGHT) {
        var iterator = _entries.entrySet().iterator();
        _retainedWeight -= iterator.next().getValue().estimatedWeight();
        iterator.remove();
      }
      return canonical;
    }

    void clear() {
      _entries.clear();
      _retainedWeight = 0;
    }

    private static long estimatedWeight(Map<?, ?> map, String canonical) {
      // Conservative retained-size estimate: map/entry/String overhead and two bytes per character, even if shared.
      long weight = 256L + 2L * canonical.length() + 256L * map.size();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        weight += 2L * ((String) entry.getKey()).length() + 2L * ((String) entry.getValue()).length();
      }
      return weight;
    }

    private record CachedJson(String canonical, long estimatedWeight) {
    }
  }
}
