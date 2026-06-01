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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StatsCollectorUtil;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ColumnarOpenStructIndexCreator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Splits an OPEN_STRUCT column into per-key materialized columns using standard Pinot index
 * creators. Dense keys become independent virtual columns; remaining keys go into a single
 * synthetic JSON column for sparse storage.
 *
 * <p>Lifecycle: instantiated by {@code BaseSegmentCreator} for OPEN_STRUCT columns. Receives
 * per-doc {@code Map<String, Object>} values via {@link #add(Map, int)}, accumulates in memory,
 * then on {@link #seal()} writes per-key column files using standard creators.
 */
public class OpenStructColumnSplitter implements ColumnarOpenStructIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenStructColumnSplitter.class);
  private static final double NO_DICTIONARY_SIZE_RATIO_THRESHOLD = 0.85;

  private final File _indexDir;
  private final String _columnName;
  private final Map<String, FieldSpec> _childFieldSpecs;
  private final OpenStructIndexConfig _config;
  private final int _maxDenseKeys;

  // Per-key accumulation
  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
  private final Map<String, Long> _totalRawBytesPerKey = new HashMap<>();
  private final Map<String, DataType> _inferredTypes = new HashMap<>();
  private int _numDocs;

  // Resolved at seal time
  @Nullable
  private Set<String> _resolvedDenseKeys;
  private final Map<String, PropertiesConfiguration> _materializedColumnMetadata = new LinkedHashMap<>();

  public OpenStructColumnSplitter(File indexDir, String columnName, FieldSpec fieldSpec,
      OpenStructIndexConfig config) {
    _indexDir = indexDir;
    _columnName = columnName;
    _config = config;
    _maxDenseKeys = config.getMaxDenseKeys();

    Map<String, FieldSpec> childFieldSpecs = null;
    if (fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
      childFieldSpecs = complexSpec.getChildFieldSpecs();
    }
    _childFieldSpecs = childFieldSpecs != null ? new HashMap<>(childFieldSpecs) : new HashMap<>();
  }

  @Override
  public void add(Object value, int docId)
      throws IOException {
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      addMap(map);
    } else {
      addMap(null);
    }
  }

  @Override
  public void add(Map<String, Object> openStructValue, int docId)
      throws IOException {
    addMap(openStructValue);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("OPEN_STRUCT index is single-value only");
  }

  /**
   * Returns the resolved dense-key set after {@link #seal()} or {@link #classify()}.
   * Returns an empty set before resolution.
   */
  public Set<String> getResolvedDenseKeys() {
    return _resolvedDenseKeys != null ? Collections.unmodifiableSet(_resolvedDenseKeys) : Set.of();
  }

  /**
   * Resolves dense vs sparse keys without writing any files. Exposed for testing and for callers
   * that need the classification independent of file output. {@link #seal()} calls this internally.
   */
  public Set<String> classify() {
    if (_resolvedDenseKeys != null) {
      return _resolvedDenseKeys;
    }
    if (_numDocs == 0 || _presenceBitmaps.isEmpty()) {
      _resolvedDenseKeys = new LinkedHashSet<>();
      return _resolvedDenseKeys;
    }
    List<String> allKeys = new ArrayList<>(_presenceBitmaps.keySet());
    allKeys.sort((a, b) -> {
      double fillA = (double) _presenceBitmaps.get(a).getCardinality() / _numDocs;
      double fillB = (double) _presenceBitmaps.get(b).getCardinality() / _numDocs;
      int cmp = Double.compare(fillB, fillA);
      return cmp != 0 ? cmp : a.compareTo(b);
    });

    double minFillRate = _config.getDenseKeyMinFillRate();
    _resolvedDenseKeys = new LinkedHashSet<>();

    Set<String> configuredDenseKeys = _config.getDenseKeys();
    for (String key : configuredDenseKeys) {
      if (_presenceBitmaps.containsKey(key) && (_maxDenseKeys < 0 || _resolvedDenseKeys.size() < _maxDenseKeys)) {
        _resolvedDenseKeys.add(key);
      }
    }

    for (String key : allKeys) {
      if (_resolvedDenseKeys.contains(key)) {
        continue;
      }
      double fillRate = (double) _presenceBitmaps.get(key).getCardinality() / _numDocs;
      if ((_maxDenseKeys < 0 || _resolvedDenseKeys.size() < _maxDenseKeys) && fillRate >= minFillRate) {
        _resolvedDenseKeys.add(key);
      }
    }
    return _resolvedDenseKeys;
  }

  private void addMap(@Nullable Map<String, Object> map) {
    if (map != null && !map.isEmpty()) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        String key = entry.getKey();
        Object rawValue = entry.getValue();
        if (rawValue == null) {
          continue;
        }
        FieldSpec keySpec = _childFieldSpecs.get(key);
        DataType valueType = keySpec != null
            ? keySpec.getDataType()
            : _inferredTypes.computeIfAbsent(key, k -> {
              DataType inferred = OpenStructNaming.inferDataType(rawValue);
              return inferred != null ? inferred : DataType.STRING;
            });
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new RoaringBitmap());
          _values.put(key, new ArrayList<>());
        }
        _presenceBitmaps.get(key).add(_numDocs);
        Object coerced;
        try {
          PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue);
          PinotDataType destType = ColumnDataType.fromDataTypeSV(valueType.getStoredType()).toPinotDataType();
          coerced = destType.convert(rawValue, sourceType);
        } catch (Exception e) {
          LOGGER.warn("OPEN_STRUCT '{}': coercion failed for key '{}' value '{}' to {}. Skipping.",
              _columnName, key, rawValue, valueType, e);
          _presenceBitmaps.get(key).remove(_numDocs);
          continue;
        }
        _values.get(key).add(coerced);

        DataType storedType = valueType.getStoredType();
        if (storedType == DataType.STRING || storedType == DataType.BYTES) {
          byte[] rawBytes = storedType == DataType.BYTES ? (byte[]) coerced
              : ((String) coerced).getBytes(StandardCharsets.UTF_8);
          _totalRawBytesPerKey.merge(key, (long) rawBytes.length, Long::sum);
        } else if (storedType == DataType.BIG_DECIMAL) {
          _totalRawBytesPerKey.merge(key, (long) BigDecimalUtils.serialize((BigDecimal) coerced).length, Long::sum);
        }
      }
    }
    _numDocs++;
  }

  @Override
  public void seal()
      throws IOException {
    classify();
    if (_resolvedDenseKeys == null || (_numDocs == 0 && _presenceBitmaps.isEmpty())) {
      return;
    }

    for (String key : _resolvedDenseKeys) {
      writeDenseKeyColumn(key);
    }

    List<String> sparseKeys = new ArrayList<>();
    for (String key : _presenceBitmaps.keySet()) {
      if (!_resolvedDenseKeys.contains(key)) {
        sparseKeys.add(key);
      }
    }
    if (!sparseKeys.isEmpty()) {
      writeSparseJsonColumn(sparseKeys);
    }

    emitParentColumnMetadata(!sparseKeys.isEmpty());
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to close — sub-creators are created and closed within seal()
  }

  @Override
  public Map<String, PropertiesConfiguration> getMaterializedColumnMetadata() {
    return _materializedColumnMetadata;
  }

  private void writeDenseKeyColumn(String key)
      throws IOException {
    String materializedCol = OpenStructNaming.materializedColumnName(_columnName, key);
    FieldSpec keySpec = _childFieldSpecs.get(key);
    DataType valueType = keySpec != null
        ? keySpec.getDataType()
        : _inferredTypes.getOrDefault(key, DataType.STRING);
    DataType storedType = valueType.getStoredType();
    RoaringBitmap presence = _presenceBitmaps.get(key);
    List<Object> values = _values.get(key);
    int numDocsForKey = presence.getCardinality();

    // Synthetic field spec for the materialized child; its default-null-value matches the value stored
    // for absent docs so column metadata stays consistent with on-disk content.
    Object defaultValue = getDefaultValue(storedType);
    DimensionFieldSpec childFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);
    childFieldSpec.setDefaultNullValue(defaultValue);

    // Collect statistics the standard way: present docs contribute their value, absent docs the default
    // (absent docs are also marked in the null vector below).
    AbstractColumnStatisticsCollector statsCollector =
        StatsCollectorUtil.createStatsCollector(childFieldSpec, null);
    int statsOrdinal = 0;
    for (int docId = 0; docId < _numDocs; docId++) {
      statsCollector.collect(presence.contains(docId) ? values.get(statsOrdinal++) : defaultValue);
    }
    statsCollector.seal();

    boolean useDictionary = shouldUseDictionary(key, storedType, statsCollector);
    boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);

    Object sortedDistinctArray = useDictionary ? statsCollector.getUniqueValuesSet() : null;
    int cardinality = useDictionary ? statsCollector.getCardinality() : numDocsForKey;

    int dictElementSize = 0;
    SegmentDictionaryCreator dictCreator = null;
    if (useDictionary) {
      dictCreator = new SegmentDictionaryCreator(
          materializedCol, storedType, new File(_indexDir,
          materializedCol + V1Constants.Dict.FILE_EXTENSION), true);
      dictCreator.build(sortedDistinctArray);
    }

    try {
      if (useDictionary) {
        int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
        int defaultDictId = dictCreator.indexOfSV(getDefaultValue(storedType));

        FixedBitSVForwardIndexWriter fwdWriter = new FixedBitSVForwardIndexWriter(
            new File(_indexDir, materializedCol + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION),
            _numDocs, numBitsPerValue);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              Object typedValue = values.get(ordinal++);
              fwdWriter.putDictId(dictCreator.indexOfSV(typedValue));
            } else {
              fwdWriter.putDictId(defaultDictId);
            }
          }
        } finally {
          fwdWriter.close();
        }
      } else {
        writeRawForwardIndex(materializedCol, storedType, presence, values);
      }

      if (enableInverted && useDictionary) {
        FieldSpec fakeFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);
        OffHeapBitmapInvertedIndexCreator invCreator = new OffHeapBitmapInvertedIndexCreator(
            _indexDir, fakeFieldSpec, cardinality, _numDocs, _numDocs);
        try {
          int defaultDictId = dictCreator.indexOfSV(getDefaultValue(storedType));
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              Object typedValue = values.get(ordinal++);
              invCreator.add(dictCreator.indexOfSV(typedValue));
            } else {
              invCreator.add(defaultDictId);
            }
          }
          invCreator.seal();
        } finally {
          invCreator.close();
        }
      }
    } finally {
      if (dictCreator != null) {
        dictElementSize = dictCreator.getNumBytesPerEntry();
        dictCreator.seal();
        dictCreator.close();
      }
    }

    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, materializedCol);
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        if (!presence.contains(docId)) {
          nullCreator.setNull(docId);
        }
      }
      nullCreator.seal();
    } finally {
      nullCreator.close();
    }

    PropertiesConfiguration props = new PropertiesConfiguration();
    FieldConfig.EncodingType encoding =
        useDictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW;
    int dictionaryElementSize = useDictionary ? dictElementSize : 0;
    BaseSegmentCreator.addColumnMetadataInfo(props, materializedCol, statsCollector, _numDocs, childFieldSpec,
        useDictionary, dictionaryElementSize, encoding, false);
    // OPEN_STRUCT-specific keys not written by addColumnMetadataInfo.
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN),
        _columnName);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, "hasNullValue"), true);
    if (enableInverted && useDictionary) {
      props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, "hasInvertedIndex"), true);
    }
    _materializedColumnMetadata.put(materializedCol, props);
  }

  private void writeRawForwardIndex(String materializedCol, DataType storedType,
      RoaringBitmap presence, List<Object> values)
      throws IOException {
    Object defaultVal = getDefaultValue(storedType);
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE: {
        SingleValueFixedByteRawIndexCreator creator = new SingleValueFixedByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            Object val = presence.contains(docId) ? values.get(ordinal++) : defaultVal;
            switch (storedType) {
              case INT:
                creator.putInt((Integer) val);
                break;
              case LONG:
                creator.putLong((Long) val);
                break;
              case FLOAT:
                creator.putFloat((Float) val);
                break;
              case DOUBLE:
                creator.putDouble((Double) val);
                break;
              default:
                break;
            }
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      case STRING: {
        int maxLen = 1;
        for (Object v : values) {
          maxLen = Math.max(maxLen, ((String) v).getBytes(StandardCharsets.UTF_8).length);
        }
        SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType, maxLen);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            creator.putString(presence.contains(docId) ? (String) values.get(ordinal++) : (String) defaultVal);
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      case BYTES: {
        int maxLen = 1;
        for (Object v : values) {
          maxLen = Math.max(maxLen, ((byte[]) v).length);
        }
        SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType, maxLen);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            creator.putBytes(presence.contains(docId) ? (byte[]) values.get(ordinal++) : (byte[]) defaultVal);
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      case BIG_DECIMAL: {
        int maxLen = 1;
        for (Object v : values) {
          maxLen = Math.max(maxLen, BigDecimalUtils.serialize((BigDecimal) v).length);
        }
        SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType, maxLen);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            creator.putBigDecimal(
                presence.contains(docId) ? (BigDecimal) values.get(ordinal++) : (BigDecimal) defaultVal);
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type for raw forward index: " + storedType);
    }
  }

  private void writeSparseJsonColumn(List<String> sparseKeys)
      throws IOException {
    String sparseCol = OpenStructNaming.sparseColumnName(_columnName);
    int maxLen = 1;
    String[] jsonPerDoc = new String[_numDocs];
    int nonNullCount = 0;
    for (int docId = 0; docId < _numDocs; docId++) {
      Map<String, Object> sparseEntries = new LinkedHashMap<>();
      for (String key : sparseKeys) {
        RoaringBitmap presence = _presenceBitmaps.get(key);
        if (presence != null && presence.contains(docId)) {
          int ordinal = presence.rank(docId) - 1;
          sparseEntries.put(key, _values.get(key).get(ordinal));
        }
      }
      if (!sparseEntries.isEmpty()) {
        try {
          String json = JsonUtils.objectToString(sparseEntries);
          jsonPerDoc[docId] = json;
          maxLen = Math.max(maxLen, json.getBytes(StandardCharsets.UTF_8).length);
          nonNullCount++;
        } catch (IOException e) {
          throw new RuntimeException("Failed to serialize sparse entries for docId " + docId, e);
        }
      }
    }

    SingleValueVarByteRawIndexCreator fwdCreator = new SingleValueVarByteRawIndexCreator(
        _indexDir, ChunkCompressionType.LZ4, sparseCol, _numDocs, DataType.STRING, maxLen);
    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, sparseCol);
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        if (jsonPerDoc[docId] != null) {
          fwdCreator.putString(jsonPerDoc[docId]);
        } else {
          fwdCreator.putString("");
          nullCreator.setNull(docId);
        }
      }
      fwdCreator.seal();
      nullCreator.seal();
    } finally {
      fwdCreator.close();
      nullCreator.close();
    }

    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.DATA_TYPE),
        DataType.STRING.name());
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
        FieldSpec.FieldType.DIMENSION.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED), true);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
        _numDocs);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.CARDINALITY),
        nonNullCount);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, "hasNullValue"), true);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN),
        _columnName);
    _materializedColumnMetadata.put(sparseCol, props);
  }

  private void emitParentColumnMetadata(boolean hasSparseColumn) {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.COLUMN_NAME),
        _columnName);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.DATA_TYPE),
        FieldSpec.DataType.OPEN_STRUCT.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
        FieldSpec.FieldType.COMPLEX.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
        true);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN),
        hasSparseColumn);
    _materializedColumnMetadata.put(_columnName, props);
  }

  private boolean shouldUseDictionary(String key, DataType storedType,
      AbstractColumnStatisticsCollector statsCollector) {
    if (_config.shouldEnableInvertedIndexForKey(key)) {
      return true;
    }
    if (!_config.shouldUseDictionaryForKey(key)) {
      return false;
    }
    int cardinality = statsCollector.getCardinality();
    if (cardinality == 0) {
      return false;
    }
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
    long dictIdFwdSize = ((long) _numDocs * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    long rawSize;
    long dictSize;
    switch (storedType) {
      case INT:
        rawSize = (long) _numDocs * Integer.BYTES;
        dictSize = (long) cardinality * Integer.BYTES;
        break;
      case LONG:
        rawSize = (long) _numDocs * Long.BYTES;
        dictSize = (long) cardinality * Long.BYTES;
        break;
      case FLOAT:
        rawSize = (long) _numDocs * Float.BYTES;
        dictSize = (long) cardinality * Float.BYTES;
        break;
      case DOUBLE:
        rawSize = (long) _numDocs * Double.BYTES;
        dictSize = (long) cardinality * Double.BYTES;
        break;
      case STRING:
      case BYTES:
      case BIG_DECIMAL: {
        long totalRawBytes = _totalRawBytesPerKey.getOrDefault(key, 0L);
        int longestElement = statsCollector.getLengthOfLongestElement();
        rawSize = Integer.BYTES + (long) (_numDocs + 1) * Integer.BYTES + totalRawBytes;
        // Conservative upper bound on the var-length dictionary: per-entry offset plus a payload
        // bounded by the longest element (actual var-length payload is <= this).
        dictSize = (long) cardinality * (Integer.BYTES + longestElement);
        break;
      }
      default:
        return true;
    }
    double ratio = (double) rawSize / (dictSize + dictIdFwdSize);
    return ratio > NO_DICTIONARY_SIZE_RATIO_THRESHOLD;
  }

  private static Object getDefaultValue(DataType storedType) {
    switch (storedType) {
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0f;
      case DOUBLE:
        return 0.0;
      case STRING:
        return "";
      case BYTES:
        return new byte[0];
      case BIG_DECIMAL:
        return BigDecimal.ZERO;
      default:
        throw new IllegalStateException("Unsupported OPEN_STRUCT stored type for default value: " + storedType);
    }
  }
}
