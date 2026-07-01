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

import com.google.common.base.Utf8;
import java.io.File;
import java.io.IOException;
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
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StatsCollectorUtil;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.openstruct.OpenStructSupportedIndexes;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ColumnarOpenStructIndexCreator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.OpenStructTypeInference;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Splits an OPEN_STRUCT column into per-key materialized columns using standard Pinot index
/// creators. Dense keys become independent virtual columns; remaining keys go into a single
/// synthetic JSON column for sparse storage.
///
/// Lifecycle: instantiated by `BaseSegmentCreator` for OPEN_STRUCT columns. Receives
/// per-doc `Map<String, Object>` values via [#add(Map, int)], accumulates in memory,
/// then on [#seal()] writes per-key column files using standard creators.
public class OpenStructColumnSplitter implements ColumnarOpenStructIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenStructColumnSplitter.class);

  private final File _indexDir;
  private final String _columnName;
  private final Map<String, FieldSpec> _childFieldSpecs;
  private final OpenStructIndexConfig _config;
  private final int _maxDenseKeys;

  // Per-key accumulation
  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
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

  /// Returns the resolved dense-key set after [#seal()] or [#classify()].
  /// Returns an empty set before resolution.
  public Set<String> getResolvedDenseKeys() {
    return _resolvedDenseKeys != null ? Collections.unmodifiableSet(_resolvedDenseKeys) : Set.of();
  }

  /// Resolves dense vs sparse keys without writing any files. Exposed for testing and for callers
  /// that need the classification independent of file output. [#seal()] calls this internally.
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
              DataType inferred = OpenStructTypeInference.inferDataType(rawValue);
              return inferred != null ? inferred : DataType.STRING;
            });
        RoaringBitmap bitmap = _presenceBitmaps.computeIfAbsent(key, k -> new RoaringBitmap());
        List<Object> values = _values.computeIfAbsent(key, k -> new ArrayList<>());
        bitmap.add(_numDocs);
        Object coerced;
        try {
          PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue);
          PinotDataType destType = ColumnDataType.fromDataTypeSV(valueType.getStoredType()).toPinotDataType();
          coerced = destType.convert(rawValue, sourceType);
        } catch (Exception e) {
          LOGGER.warn("OPEN_STRUCT '{}': coercion failed for key '{}' value '{}' to {}. Skipping.",
              _columnName, key, rawValue, valueType, e);
          bitmap.remove(_numDocs);
          continue;
        }
        values.add(coerced);
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

    // Synthetic field spec for the materialized child. Its natural Pinot dimension null value is the value
    // stored for absent docs, so column metadata stays consistent with on-disk content.
    DimensionFieldSpec childFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);
    Object defaultValue = childFieldSpec.getDefaultNullValue();

    // Collect statistics the standard way: present docs contribute their value, absent docs the default
    // (absent docs are also marked in the null vector below).
    AbstractColumnStatisticsCollector statsCollector =
        StatsCollectorUtil.createStatsCollector(childFieldSpec, null);
    int statsOrdinal = 0;
    for (int docId = 0; docId < _numDocs; docId++) {
      statsCollector.collect(presence.contains(docId) ? values.get(statsOrdinal++) : defaultValue);
    }
    statsCollector.seal();

    // Build per-key index configuration from the key's FieldConfig (falling back to the default), then apply
    // the OPEN_STRUCT inverted-on default. No TableConfig/Schema required.
    FieldConfig keyFieldConfig = _config.getValueFieldConfig(key);
    if (keyFieldConfig == null) {
      keyFieldConfig = _config.getDefaultValueFieldConfig();
    }
    boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);
    FieldIndexConfigs configsForDecision = new FieldIndexConfigs.Builder(
        FieldIndexConfigsUtil.fromFieldConfig(keyFieldConfig, childFieldSpec))
        .add(StandardIndexes.inverted(), enableInverted ? IndexConfig.ENABLED : IndexConfig.DISABLED)
        .build();

    boolean useDictionary = resolveUseDictionary(childFieldSpec, configsForDecision, statsCollector);

    // Reconcile dictionary + forward encoding with the final decision (mirrors BaseSegmentCreator.adaptConfig);
    // ForwardIndexCreatorFactory selects dict-vs-raw from the forward config's EncodingType. A compression codec
    // applies only to the raw forward format (LZ4 preserves the dense child's current on-disk layout); attaching
    // one to a dictionary-encoded forward is rejected by ForwardIndexType.validate.
    ForwardIndexConfig.Builder forwardBuilder = new ForwardIndexConfig.Builder(
        useDictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW);
    if (!useDictionary) {
      forwardBuilder.withCompressionCodec(FieldConfig.CompressionCodec.LZ4);
    }
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder(configsForDecision)
        .add(StandardIndexes.dictionary(),
            useDictionary ? DictionaryIndexConfig.DEFAULT : DictionaryIndexConfig.DISABLED)
        .add(StandardIndexes.forward(), forwardBuilder.build())
        .build();

    int dictElementSize = writeColumnIndexes(materializedCol, storedType, presence, values,
        defaultValue, statsCollector, useDictionary, fieldIndexConfigs, childFieldSpec);

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
    BaseSegmentCreator.addColumnMetadataInfo(props, materializedCol, statsCollector, _numDocs, childFieldSpec,
        useDictionary, dictElementSize, encoding, false);
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

  /// Decides dictionary vs raw encoding for a materialized child column, mirroring the three steps of
  /// `BaseSegmentCreator.createDictionaryForColumn` with standard default flags (optimizeDictionary
  /// off => dictionary unless explicitly disabled and not required by an enabled index).
  private boolean resolveUseDictionary(FieldSpec childFieldSpec, FieldIndexConfigs fieldIndexConfigs,
      AbstractColumnStatisticsCollector statsCollector) {
    if (DictionaryIndexConfig.requiresDictionary(childFieldSpec, fieldIndexConfigs)) {
      return true;
    }
    if (fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isDisabled()) {
      return false;
    }
    return DictionaryIndexType.ignoreDictionaryOverride(false, false,
        IndexingConfig.DEFAULT_NO_DICTIONARY_SIZE_RATIO_THRESHOLD, null, childFieldSpec, fieldIndexConfigs,
        statsCollector.getCardinality(), statsCollector.getTotalNumberOfEntries());
  }

  /// Writes the dictionary (when used) plus all vetted, enabled indexes for a materialized child column through
  /// the standard index-creator family, driven from a single per-doc loop. Returns the dictionary element size in
  /// bytes (0 when raw-encoded), for column metadata. The dictionary is built separately because its build
  /// lifecycle is CUSTOM and it supplies the dictIds the per-row creators consume.
  private int writeColumnIndexes(String materializedCol, DataType storedType, RoaringBitmap presence,
      List<Object> values, Object defaultValue, AbstractColumnStatisticsCollector statsCollector,
      boolean useDictionary, FieldIndexConfigs fieldIndexConfigs, FieldSpec childFieldSpec)
      throws IOException {
    int dictElementSize = 0;
    SegmentDictionaryCreator dictCreator = null;
    try {
      if (useDictionary) {
        dictCreator = new SegmentDictionaryCreator(materializedCol, storedType,
            new File(_indexDir, materializedCol + V1Constants.Dict.FILE_EXTENSION), true);
        dictCreator.build(statsCollector.getUniqueValuesSet());
      }

      // Index-creation context built from the sealed collector (a ColumnShape) — no TableConfig required.
      IndexCreationContext context =
          new IndexCreationContext.Builder(_indexDir, null, statsCollector, useDictionary, false)
              .withOnHeap(false).build();

      List<IndexCreator> creators = new ArrayList<>();
      try {
        for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
          if (indexType.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
            continue;   // excludes dictionary (lifecycle CUSTOM), built separately above
          }
          if (!OpenStructSupportedIndexes.ALLOWED_PRETTY_NAMES.contains(indexType.getPrettyName())) {
            continue;   // non-vetted indexes already rejected at table-config validation; defensive backstop
          }
          IndexCreator creator = createColumnIndexCreator(indexType, context, fieldIndexConfigs, materializedCol,
              childFieldSpec);
          if (creator != null) {
            creators.add(creator);
          }
        }

        int ordinal = 0;
        for (int docId = 0; docId < _numDocs; docId++) {
          Object value = presence.contains(docId) ? values.get(ordinal++) : defaultValue;
          int dictId = useDictionary ? dictCreator.indexOfSV(value) : -1;
          for (IndexCreator creator : creators) {
            creator.add(value, dictId);
          }
        }
        for (IndexCreator creator : creators) {
          creator.seal();
        }
      } finally {
        for (IndexCreator creator : creators) {
          creator.close();
        }
        if (dictCreator != null) {
          dictElementSize = dictCreator.getNumBytesPerEntry();
          dictCreator.seal();
        }
      }
    } finally {
      if (dictCreator != null) {
        dictCreator.close();
      }
    }
    return dictElementSize;
  }

  @Nullable
  private static <C extends IndexConfig> IndexCreator createColumnIndexCreator(IndexType<C, ?, ?> indexType,
      IndexCreationContext context, FieldIndexConfigs fieldIndexConfigs, String materializedCol,
      FieldSpec childFieldSpec)
      throws IOException {
    // Materialized child columns exist in no schema/TableConfig, so the standard table-config-time validation
    // never sees them. Run the index type's own guards here against the resolved child FieldSpec (e.g. range
    // rejects a non-numeric column without a dictionary) so misconfigurations fail with the canonical message
    // instead of crashing opaquely inside the creator. validate() internally no-ops when the index is disabled.
    indexType.validate(fieldIndexConfigs, childFieldSpec, null);
    C config = fieldIndexConfigs.getConfig(indexType);
    if (!config.isEnabled() || !indexType.shouldCreateIndex(context, config)) {
      return null;
    }
    try {
      return indexType.createIndexCreator(context, config);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to create " + indexType.getPrettyName() + " creator for: " + materializedCol, e);
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
          maxLen = Math.max(maxLen, Utf8.encodedLength(json));
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
}
