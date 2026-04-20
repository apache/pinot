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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.local.segment.index.map.NullDataSource;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Data source for MAP columns with sparse map index. Wraps a {@link ColumnarMapIndexReader} and exposes it for
 * query access.
 *
 * <p>Implements {@link MapDataSource} so that {@code metrics['key']} expressions handled by
 * {@code ItemTransformFunction} can resolve per-key {@link DataSource} instances via
 * {@link #getKeyDataSource(String)}.  Each per-key data source wraps a
 * {@link ColumnarMapKeyForwardIndexReader} that delegates typed reads directly to the underlying
 * {@link ColumnarMapIndexReader}.
 */
public class ColumnarMapDataSource extends BaseDataSource implements MapDataSource {

  private final ColumnarMapIndexReader _columnarMapIndexReader;
  private final Map<String, DataSource> _keyDataSourceCache = new ConcurrentHashMap<>();

  /**
   * Constructs a ColumnarMapDataSource for an immutable segment.
   */
  public ColumnarMapDataSource(ColumnMetadata columnMetadata, ColumnarMapIndexReader columnarMapIndexReader) {
    super(new ColumnarMapDataSourceMetadata(columnMetadata),
        buildContainerWithForwardIndex(columnarMapIndexReader));
    _columnarMapIndexReader = columnarMapIndexReader;
  }

  /**
   * Constructs a ColumnarMapDataSource for a mutable (real-time) segment.
   */
  public ColumnarMapDataSource(FieldSpec fieldSpec, int numDocs, ColumnarMapIndexReader columnarMapIndexReader) {
    super(new MutableColumnarMapDataSourceMetadata(fieldSpec, numDocs),
        buildContainerWithForwardIndex(columnarMapIndexReader));
    _columnarMapIndexReader = columnarMapIndexReader;
  }

  private static ColumnIndexContainer buildContainerWithForwardIndex(ColumnarMapIndexReader reader) {
    return new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), new ColumnarMapForwardIndexReader(reader))
        .build();
  }

  /**
   * Returns the ColumnarMapIndexReader for this column.
   */
  public ColumnarMapIndexReader getColumnarMapIndexReader() {
    return _columnarMapIndexReader;
  }

  // ---- MapDataSource implementation ----

  /**
   * MAP columns with sparse map index use per-key typed storage rather than the homogeneous
   * {@link ComplexFieldSpec.MapFieldSpec}. Returns {@code null} because the types are incompatible.
   */
  @Nullable
  @Override
  public ComplexFieldSpec.MapFieldSpec getFieldSpec() {
    return null;
  }

  /**
   * Returns a per-key {@link DataSource} whose forward index delegates to this column's
   * {@link ColumnarMapIndexReader} for the requested key.
   *
   * <p>For an unknown key (not present in this segment's index), returns a
   * {@link NullDataSource} that yields the type's default value for every doc. Matches the
   * contract of {@link org.apache.pinot.segment.local.segment.index.map.BaseMapDataSource}
   * so callers (e.g. {@code ProjectionBlock}, {@code ItemTransformFunction}) never need a
   * null guard.
   */
  @Override
  public DataSource getKeyDataSource(String key) {
    DataSource cached = _keyDataSourceCache.get(key);
    if (cached != null) {
      return cached;
    }
    DataSource ds = buildKeyDataSource(key);
    if (ds == null) {
      ds = new NullDataSource(key);
    }
    DataSource existing = _keyDataSourceCache.putIfAbsent(key, ds);
    return existing != null ? existing : ds;
  }

  @Override
  public Map<String, DataSource> getKeyDataSources() {
    Map<String, DataSource> result = new HashMap<>();
    for (String key : _columnarMapIndexReader.getKeys()) {
      result.put(key, getKeyDataSource(key));
    }
    return result;
  }

  @Override
  public DataSourceMetadata getKeyDataSourceMetadata(String key) {
    throw new UnsupportedOperationException("getKeyDataSourceMetadata not supported for MAP with sparse map index");
  }

  @Override
  public ColumnIndexContainer getKeyIndexContainer(String key) {
    throw new UnsupportedOperationException("getKeyIndexContainer not supported for MAP with sparse map index");
  }

  private DataSource buildKeyDataSource(String key) {
    FieldSpec.DataType keyType = _columnarMapIndexReader.getKeyValueType(key);

    // Check if this is a sparse key (type is known from SPMX metadata but no per-key data)
    if (keyType == null) {
      return null;
    }

    // Mutable segment: use per-key FixedByteSVMutableForwardIndex directly (O(1) lock-free reads)
    if (_columnarMapIndexReader instanceof MutableColumnarMapIndexImpl) {
      return buildMutableKeyDataSource(key, keyType, (MutableColumnarMapIndexImpl) _columnarMapIndexReader);
    }

    if (_columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      ImmutableColumnarMapIndexReader immutableReader = (ImmutableColumnarMapIndexReader) _columnarMapIndexReader;
      byte tierFlag = immutableReader.getTierFlag(key);

      if (tierFlag == ImmutableColumnarMapIndexReader.TIER_SPARSE) {
        // Sparse key: build DataSource backed by sidecar JSON blob reader
        return buildSparseKeyDataSource(key, keyType, immutableReader);
      }
    }

    // Dense key (immutable): standard path
    DimensionFieldSpec keyFieldSpec = new DimensionFieldSpec(key, keyType, true);

    // Try to use pre-built dictionary and dictId reader from immutable reader
    ColumnarMapKeyDictionary keyDictionary = null;
    FixedBitIntReaderWriter dictIdReader = null;
    if (_columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      ImmutableColumnarMapIndexReader immutableReader = (ImmutableColumnarMapIndexReader) _columnarMapIndexReader;
      keyDictionary = immutableReader.getKeyDictionary(key);
      dictIdReader = immutableReader.getDictIdReader(key);
    }

    // Fallback: build dictionary from inverted index if pre-built not available
    if (keyDictionary == null) {
      String[] distinctValues = _columnarMapIndexReader.getDistinctValuesForKey(key);
      if (distinctValues != null) {
        String defaultValue = ColumnarMapKeyDictionary.getDefaultValueString(keyType);
        distinctValues = mergeDefaultValue(distinctValues, defaultValue, keyType);
        keyDictionary = new ColumnarMapKeyDictionary(keyType, distinctValues);
      }
    }

    // Determine if this is a dense key (direct docId access, no rank())
    boolean isDense = false;
    if (_columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      isDense = ((ImmutableColumnarMapIndexReader) _columnarMapIndexReader).getTierFlag(key)
          == ImmutableColumnarMapIndexReader.TIER_DENSE;
    }

    // For dense dictionary-encoded keys, use FixedBitSVForwardIndexReaderV2 (V2 reader)
    // which has SIMD-optimized read32() for contiguous docIds — same fast path as regular columns.
    ForwardIndexReader<?> keyReader;
    if (isDense && keyDictionary != null && _columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      ImmutableColumnarMapIndexReader immutableReader = (ImmutableColumnarMapIndexReader) _columnarMapIndexReader;
      PinotDataBuffer dictIdFwdBuffer = immutableReader.getDictIdFwdBuffer(key);
      int numBitsPerDictId = immutableReader.getNumBitsPerDictId(key);
      if (dictIdFwdBuffer != null && numBitsPerDictId > 0) {
        int numDocs = getDataSourceMetadata().getNumDocs();
        keyReader = new FixedBitSVForwardIndexReaderV2(dictIdFwdBuffer, numDocs, numBitsPerDictId);
      } else {
        keyReader = new ColumnarMapKeyForwardIndexReader(_columnarMapIndexReader, key, keyType, keyDictionary,
            dictIdReader, _columnarMapIndexReader.getPresenceBitmap(key), isDense);
      }
    } else {
      keyReader = new ColumnarMapKeyForwardIndexReader(_columnarMapIndexReader, key, keyType, keyDictionary,
          dictIdReader, _columnarMapIndexReader.getPresenceBitmap(key), isDense);
    }

    ColumnIndexContainer.FromMap.Builder containerBuilder =
        new ColumnIndexContainer.FromMap.Builder().with(StandardIndexes.forward(), keyReader);
    if (keyDictionary != null) {
      containerBuilder.with(StandardIndexes.dictionary(), keyDictionary);
    }
    // Add per-key inverted index if available (enables fast filter path in MapFilterOperator)
    if (_columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      ImmutableColumnarMapIndexReader immutableReader = (ImmutableColumnarMapIndexReader) _columnarMapIndexReader;
      if (immutableReader.hasInvertedIndex(key) && keyDictionary != null) {
        containerBuilder.with(StandardIndexes.inverted(),
            new ColumnarMapKeyInvertedIndexReader(immutableReader, key, keyDictionary));
      }
      // Add null value vector for dense keys (docs where key is absent)
      ImmutableRoaringBitmap nullBitmap = immutableReader.getNullBitmap(key);
      if (nullBitmap != null) {
        containerBuilder.with(StandardIndexes.nullValueVector(), bitmapNullValueVector(nullBitmap));
      }
    }
    ColumnIndexContainer keyContainer = containerBuilder.build();

    int numDocs = getDataSourceMetadata().getNumDocs();
    int cardinality = keyDictionary != null ? keyDictionary.length() : 0;
    return new BaseDataSource(new KeyDataSourceMetadata(keyFieldSpec, numDocs, cardinality), keyContainer) {
    };
  }

  @Nullable
  private DataSource buildMutableKeyDataSource(String key, FieldSpec.DataType keyType,
      MutableColumnarMapIndexImpl mutableReader) {
    MutableForwardIndex fwdIndex = mutableReader.getPerKeyForwardIndex(key);
    if (fwdIndex == null) {
      // Key has a known type but was never ingested — lazily initialize storage so it's queryable
      // (all docs will read default values from the zeroed forward index)
      mutableReader.ensureKeyStorage(key);
      fwdIndex = mutableReader.getPerKeyForwardIndex(key);
      if (fwdIndex == null) {
        return null;
      }
    }

    DimensionFieldSpec keyFieldSpec = new DimensionFieldSpec(key, keyType, true);
    int numDocs = getDataSourceMetadata().getNumDocs();

    ColumnIndexContainer.FromMap.Builder containerBuilder =
        new ColumnIndexContainer.FromMap.Builder().with(StandardIndexes.forward(), fwdIndex);

    int cardinality = 0;
    if (mutableReader.isDictionaryEncodedKey(key)) {
      MutableDictionary dict = mutableReader.getPerKeyDictionary(key);
      if (dict != null) {
        containerBuilder.with(StandardIndexes.dictionary(), dict);
        cardinality = dict.length();
      }
      // Wire dictId-based inverted index for InvertedIndexFilterOperator
      ColumnarMapRealtimeInvertedIndex invertedIndex = mutableReader.getPerKeyInvertedIndex(key);
      if (invertedIndex != null) {
        containerBuilder.with(StandardIndexes.inverted(), invertedIndex);
      }
    }

    // Add null value vector for mutable keys (complement of presence bitmap)
    ImmutableRoaringBitmap presenceBitmap = mutableReader.getPresenceBitmap(key);
    if (presenceBitmap != null) {
      int mutableNumDocs = numDocs;
      MutableRoaringBitmap nullBitmap = new MutableRoaringBitmap();
      nullBitmap.add(0L, (long) mutableNumDocs);
      nullBitmap.andNot(presenceBitmap);
      containerBuilder.with(StandardIndexes.nullValueVector(),
          bitmapNullValueVector(nullBitmap.toImmutableRoaringBitmap()));
    }

    ColumnIndexContainer keyContainer = containerBuilder.build();
    return new BaseDataSource(new KeyDataSourceMetadata(keyFieldSpec, numDocs, cardinality), keyContainer) {
    };
  }

  /**
   * Builds a DataSource for a sparse-tier key backed by the sidecar JSON blob reader.
   * The forward index reader extracts the key's value from each doc's JSON blob.
   */
  private DataSource buildSparseKeyDataSource(String key, FieldSpec.DataType keyType,
      ImmutableColumnarMapIndexReader immutableReader) {
    DimensionFieldSpec keyFieldSpec = new DimensionFieldSpec(key, keyType, true);
    int numDocs = getDataSourceMetadata().getNumDocs();

    // Create a ForwardIndexReader that reads from the sidecar JSON blob
    // TODO(A11, columnar-map-todos.md): extract this anonymous class to a top-level
    // ColumnarMapSparseKeyForwardIndexReader so the per-doc JSON parse logic is unit-testable
    // and the perf fix (cache parsed map per docId in ForwardIndexReaderContext, or stream the
    // JSON token-by-token) can land independently.
    ForwardIndexReader<ForwardIndexReaderContext> sparseReader = new ForwardIndexReader<ForwardIndexReaderContext>() {
      @Override
      public boolean isDictionaryEncoded() {
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return true;
      }

      @Override
      public FieldSpec.DataType getStoredType() {
        return keyType.getStoredType();
      }

      @Override
      public String getString(int docId, ForwardIndexReaderContext context) {
        String json = immutableReader.getSparseJsonBlob(docId);
        if (json == null) {
          return ColumnarMapKeyDictionary.getDefaultValueString(keyType);
        }
        Map<String, Object> map;
        try {
          @SuppressWarnings("unchecked")
          Map<String, Object> parsed = org.apache.pinot.spi.utils.JsonUtils.stringToObject(json, Map.class);
          map = parsed;
        } catch (Exception e) {
          throw new RuntimeException("Failed to parse sparse map sidecar JSON for column='" + getColumnName()
              + "' key='" + key + "' docId=" + docId, e);
        }
        Object val = map.get(key);
        return val != null ? String.valueOf(val) : ColumnarMapKeyDictionary.getDefaultValueString(keyType);
      }

      @Override
      public int getInt(int docId, ForwardIndexReaderContext context) {
        String val = getString(docId, context);
        if (val.isEmpty()) {
          return 0;
        }
        try {
          return Integer.parseInt(val);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Sparse map value not parseable as INT for column='" + getColumnName()
              + "' key='" + key + "' docId=" + docId + " value='" + val + "'", e);
        }
      }

      @Override
      public long getLong(int docId, ForwardIndexReaderContext context) {
        String val = getString(docId, context);
        if (val.isEmpty()) {
          return 0L;
        }
        try {
          return Long.parseLong(val);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Sparse map value not parseable as LONG for column='" + getColumnName()
              + "' key='" + key + "' docId=" + docId + " value='" + val + "'", e);
        }
      }

      @Override
      public float getFloat(int docId, ForwardIndexReaderContext context) {
        String val = getString(docId, context);
        if (val.isEmpty()) {
          return 0.0f;
        }
        try {
          return Float.parseFloat(val);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Sparse map value not parseable as FLOAT for column='" + getColumnName()
              + "' key='" + key + "' docId=" + docId + " value='" + val + "'", e);
        }
      }

      @Override
      public double getDouble(int docId, ForwardIndexReaderContext context) {
        String val = getString(docId, context);
        if (val.isEmpty()) {
          return 0.0;
        }
        try {
          return Double.parseDouble(val);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Sparse map value not parseable as DOUBLE for column='" + getColumnName()
              + "' key='" + key + "' docId=" + docId + " value='" + val + "'", e);
        }
      }

      @Override
      public void close() {
      }
    };

    ColumnIndexContainer keyContainer = new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), sparseReader)
        .build();

    return new BaseDataSource(new KeyDataSourceMetadata(keyFieldSpec, numDocs, 0), keyContainer) {
    };
  }

  private static String[] mergeDefaultValue(String[] sortedValues, String defaultValue,
      FieldSpec.DataType keyType) {
    Comparator<String> cmp = ColumnarMapKeyDictionary.getComparator(keyType);
    int insertionPoint = cmp != null
        ? java.util.Arrays.binarySearch(sortedValues, defaultValue, cmp)
        : java.util.Arrays.binarySearch(sortedValues, defaultValue);
    if (insertionPoint >= 0) {
      // Default value already in the array
      return sortedValues;
    }
    // Insert at the correct position to maintain sorted order
    int pos = -(insertionPoint + 1);
    String[] merged = new String[sortedValues.length + 1];
    System.arraycopy(sortedValues, 0, merged, 0, pos);
    merged[pos] = defaultValue;
    System.arraycopy(sortedValues, pos, merged, pos + 1, sortedValues.length - pos);
    return merged;
  }

  // ---- Per-key DataSourceMetadata ----

  private static class KeyDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _keyFieldSpec;
    private final int _numDocs;
    private final int _cardinality;

    KeyDataSourceMetadata(FieldSpec keyFieldSpec, int numDocs, int cardinality) {
      _keyFieldSpec = keyFieldSpec;
      _numDocs = numDocs;
      _cardinality = cardinality;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _keyFieldSpec;
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
      return -1;
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
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public int getCardinality() {
      return _cardinality;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }

  // ---- Metadata for immutable segments ----

  private static class ColumnarMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;
    private final int _numValues;
    private final PartitionFunction _partitionFunction;
    private final Set<Integer> _partitions;

    ColumnarMapDataSourceMetadata(ColumnMetadata columnMetadata) {
      _fieldSpec = columnMetadata.getFieldSpec();
      _numDocs = columnMetadata.getTotalDocs();
      _numValues = columnMetadata.getTotalNumberOfEntries();
      _partitionFunction = columnMetadata.getPartitionFunction();
      _partitions = columnMetadata.getPartitions();
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
      return -1;
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

    @Override
    public int getCardinality() {
      return 0;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }

  // ---- Metadata for mutable (real-time) segments ----

  private static class MutableColumnarMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    MutableColumnarMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
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
      return -1;
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
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public int getCardinality() {
      return 0;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }

  private static NullValueVectorReader bitmapNullValueVector(ImmutableRoaringBitmap nullBitmap) {
    return new NullValueVectorReader() {
      @Override
      public boolean isNull(int docId) {
        return nullBitmap.contains(docId);
      }

      @Override
      public ImmutableRoaringBitmap getNullBitmap() {
        return nullBitmap;
      }
    };
  }
}
