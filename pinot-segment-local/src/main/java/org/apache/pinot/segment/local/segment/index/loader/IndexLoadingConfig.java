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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.OnHeapDictionaryConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;
  public static final String READ_MODE_KEY = "readMode";
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexLoadingConfig.class);

  private InstanceDataManagerConfig _instanceDataManagerConfig = null;
  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private List<String> _sortedColumns = Collections.emptyList();
  private Set<String> _invertedIndexColumns = new HashSet<>();
  private Set<String> _rangeIndexColumns = new HashSet<>();
  private int _rangeIndexVersion = RangeIndexConfig.DEFAULT.getVersion();
  private Set<String> _textIndexColumns = new HashSet<>();
  private Set<String> _fstIndexColumns = new HashSet<>();
  private FSTType _fstIndexType = FSTType.LUCENE;
  private Map<String, JsonIndexConfig> _jsonIndexConfigs = new HashMap<>();
  private Map<String, H3IndexConfig> _h3IndexConfigs = new HashMap<>();
  private Map<String, VectorIndexConfig> _vectorIndexConfigs = new HashMap<>();
  private Set<String> _noDictionaryColumns = new HashSet<>(); // TODO: replace this by _noDictionaryConfig.
  private final Map<String, String> _noDictionaryConfig = new HashMap<>();
  private final Set<String> _varLengthDictionaryColumns = new HashSet<>();
  private Set<String> _onHeapDictionaryColumns = new HashSet<>();
  private Set<String> _forwardIndexDisabledColumns = new HashSet<>();
  private Map<String, BloomFilterConfig> _bloomFilterConfigs = new HashMap<>();
  private Map<String, OnHeapDictionaryConfig> _onHeapDictionaryConfigs = new HashMap<>();
  private boolean _enableDynamicStarTreeCreation;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private boolean _enableDefaultStarTree;
  private Map<String, CompressionCodec> _compressionConfigs = new HashMap<>();
  private Map<String, FieldIndexConfigs> _indexConfigsByColName = new HashMap<>();

  private SegmentVersion _segmentVersion;
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private boolean _isRealtimeOffHeapAllocation;
  private boolean _isDirectRealtimeOffHeapAllocation;
  private String _segmentStoreURI;
  private boolean _errorOnColumnBuildFailure;

  // constructed from FieldConfig
  private Map<String, Map<String, String>> _columnProperties = new HashMap<>();

  @Nullable
  private TableConfig _tableConfig;
  private Schema _schema;
  private String _tableDataDir;
  private String _segmentDirectoryLoader;
  private String _segmentTier;

  private String _instanceId;
  private Map<String, Map<String, String>> _instanceTierConfigs;
  private boolean _dirty = true;
  private Set<String> _knownColumns = null;

  /**
   * NOTE: This step might modify the passed in table config and schema.
   */
  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig,
      @Nullable Schema schema) {
    extractFromInstanceConfig(instanceDataManagerConfig);
    extractFromTableConfigAndSchema(tableConfig, schema);
  }

  @VisibleForTesting
  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    this(instanceDataManagerConfig, tableConfig, null);
  }

  public IndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    extractFromTableConfigAndSchema(tableConfig, schema);
  }

  public IndexLoadingConfig() {
  }

  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  private void extractFromTableConfigAndSchema(TableConfig tableConfig, @Nullable Schema schema) {
    if (schema != null) {
      TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);
    }
    _tableConfig = tableConfig;
    _schema = schema;

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    String tableReadMode = indexingConfig.getLoadMode();
    if (tableReadMode != null) {
      _readMode = ReadMode.getEnum(tableReadMode);
    }

    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      _sortedColumns = sortedColumns;
    }

    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (invertedIndexColumns != null) {
      _invertedIndexColumns.addAll(invertedIndexColumns);
    }

    // Ignore jsonIndexColumns when jsonIndexConfigs is configured
    Map<String, JsonIndexConfig> jsonIndexConfigs = indexingConfig.getJsonIndexConfigs();
    if (jsonIndexConfigs != null) {
      _jsonIndexConfigs = jsonIndexConfigs;
    } else {
      List<String> jsonIndexColumns = indexingConfig.getJsonIndexColumns();
      if (jsonIndexColumns != null) {
        _jsonIndexConfigs = new HashMap<>();
        for (String jsonIndexColumn : jsonIndexColumns) {
          _jsonIndexConfigs.put(jsonIndexColumn, new JsonIndexConfig());
        }
      }
    }

    List<String> rangeIndexColumns = indexingConfig.getRangeIndexColumns();
    if (rangeIndexColumns != null) {
      _rangeIndexColumns.addAll(rangeIndexColumns);
    }

    _rangeIndexVersion = indexingConfig.getRangeIndexVersion();

    _fstIndexType = indexingConfig.getFSTIndexType();

    List<String> bloomFilterColumns = indexingConfig.getBloomFilterColumns();
    if (bloomFilterColumns != null) {
      for (String bloomFilterColumn : bloomFilterColumns) {
        _bloomFilterConfigs.put(bloomFilterColumn, new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 0, false));
      }
    }
    Map<String, BloomFilterConfig> bloomFilterConfigs = indexingConfig.getBloomFilterConfigs();
    if (bloomFilterConfigs != null) {
      _bloomFilterConfigs.putAll(bloomFilterConfigs);
    }

    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    if (noDictionaryColumns != null) {
      _noDictionaryColumns.addAll(noDictionaryColumns);
    }

    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        _columnProperties.put(fieldConfig.getName(), fieldConfig.getProperties());
      }
    }

    extractCompressionConfigs(tableConfig);
    extractTextIndexColumnsFromTableConfig(tableConfig);
    extractFSTIndexColumnsFromTableConfig(tableConfig);
    extractH3IndexConfigsFromTableConfig(tableConfig);
    extractVectorIndexConfigsFromTableConfig(tableConfig);
    extractForwardIndexDisabledColumnsFromTableConfig(tableConfig);

    Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
    if (noDictionaryConfig != null) {
      _noDictionaryConfig.putAll(noDictionaryConfig);
    }

    List<String> varLengthDictionaryColumns = indexingConfig.getVarLengthDictionaryColumns();
    if (varLengthDictionaryColumns != null) {
      _varLengthDictionaryColumns.addAll(varLengthDictionaryColumns);
    }

    List<String> onHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns();
    Map<String, OnHeapDictionaryConfig> onHeapDictionaryConfigMap = indexingConfig.getOnHeapDictionaryConfigs();
    if (onHeapDictionaryColumns != null) {
      for (String col : onHeapDictionaryColumns) {
        _onHeapDictionaryColumns.add(col);
        if (onHeapDictionaryConfigMap != null && onHeapDictionaryConfigMap.containsKey(col)) {
          _onHeapDictionaryConfigs.put(col, onHeapDictionaryConfigMap.get(col));
        }
      }
    }

    String tableSegmentVersion = indexingConfig.getSegmentFormatVersion();
    if (tableSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
    }

    String columnMinMaxValueGeneratorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode != null) {
      _columnMinMaxValueGeneratorMode =
          ColumnMinMaxValueGeneratorMode.valueOf(columnMinMaxValueGeneratorMode.toUpperCase());
    }

    refreshIndexConfigs();
  }

  public void refreshIndexConfigs() {
    TableConfig tableConfig = getTableConfigWithTierOverwrites();
    // Accessing the index configs for single-column index is handled by IndexType.getConfig() as defined in index-spi.
    // As the tableConfig is overwritten with tier specific configs, IndexType.getConfig() can access the tier
    // specific index configs transparently.
    _indexConfigsByColName = calculateIndexConfigsByColName(tableConfig, inferSchema());
    // Accessing the StarTree index configs is not handled by IndexType.getConfig(), so we manually update them.
    if (tableConfig != null) {
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      _enableDynamicStarTreeCreation = indexingConfig.isEnableDynamicStarTreeCreation();
      _starTreeIndexConfigs = indexingConfig.getStarTreeIndexConfigs();
      _enableDefaultStarTree = indexingConfig.isEnableDefaultStarTree();
    }
    _dirty = false;
  }

  /**
   * Calculates the map from column to {@link FieldIndexConfigs}, merging the information related to older configs (
   * which is also heavily used by tests) and the one included in the TableConfig (in case the latter is not null).
   *
   * This method does not modify the result of {@link #getFieldIndexConfigByColName()} or
   * {@link #getFieldIndexConfigByColName()}. To do so, call {@link #refreshIndexConfigs()}.
   *
   * The main difference between this method and
   * {@link FieldIndexConfigsUtil#createIndexConfigsByColName(TableConfig, Schema)} is that the former relays
   * on the TableConfig, while this method can be used even when the {@link IndexLoadingConfig} was configured by
   * calling the setter methods.
   */
  public Map<String, FieldIndexConfigs> calculateIndexConfigsByColName() {
    return calculateIndexConfigsByColName(getTableConfigWithTierOverwrites(), inferSchema());
  }

  private Map<String, FieldIndexConfigs> calculateIndexConfigsByColName(@Nullable TableConfig tableConfig,
      Schema schema) {
    return FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema, this::getDeserializer);
  }

  private <C extends IndexConfig> ColumnConfigDeserializer<C> getDeserializer(IndexType<C, ?, ?> indexType) {
    ColumnConfigDeserializer<C> deserializer;

    ColumnConfigDeserializer<C> stdDeserializer = indexType::getConfig;
    if (indexType instanceof ConfigurableFromIndexLoadingConfig) {

      @SuppressWarnings("unchecked")
      Map<String, C> fromIndexLoadingConfig =
          ((ConfigurableFromIndexLoadingConfig<C>) indexType).fromIndexLoadingConfig(this);

      if (_schema == null || _tableConfig == null) {
        LOGGER.debug("Ignoring default deserializers given that there is no schema [{}] or table config [{}]. Using "
            + "indexLoadingConfig for indexType: {}", _schema == null, _tableConfig == null, indexType);
        deserializer = IndexConfigDeserializer.fromMap(table -> fromIndexLoadingConfig);
      } else if (_segmentTier == null) {
        deserializer =
            IndexConfigDeserializer.fromMap(table -> fromIndexLoadingConfig).withFallbackAlternative(stdDeserializer);
      } else {
        // No need to fall back to fromIndexLoadingConfig which contains index configs for default tier, when looking
        // for tier specific index configs.
        deserializer = stdDeserializer;
      }
    } else {
      if (_schema == null || _tableConfig == null) {
        LOGGER.debug(
            "Ignoring default deserializers given that there is no schema [{}] or table config [{}]. Using default "
                + "configs for indexType: {}", _schema == null, _tableConfig == null, indexType);
        deserializer = (tableConfig, schema) -> getAllKnownColumns().stream()
            .collect(Collectors.toMap(Function.identity(), col -> indexType.getDefaultConfig()));
      } else {
        deserializer = stdDeserializer;
      }
    }
    return deserializer;
  }

  private TableConfig getTableConfigWithTierOverwrites() {
    return (_segmentTier == null || _tableConfig == null) ? _tableConfig
        : TableConfigUtils.overwriteTableConfigForTier(_tableConfig, _segmentTier);
  }

  private Schema inferSchema() {
    if (_schema != null) {
      return _schema;
    }
    Schema schema = new Schema();
    for (String column : getAllKnownColumns()) {
      schema.addField(new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true));
    }
    return schema;
  }

  /**
   * Extracts compressionType for each column. Populates a map containing column name as key and compression type as
   * value. This map will only contain the compressionType overrides, and it does not correspond to the default value
   * of compressionType (derived using SegmentColumnarIndexCreator.getColumnCompressionType())  used for a column.
   * Note that only RAW forward index columns will be populated in this map.
   * @param tableConfig table config
   */
  private void extractCompressionConfigs(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return;
    }

    for (FieldConfig fieldConfig : fieldConfigList) {
      String column = fieldConfig.getName();
      if (fieldConfig.getCompressionCodec() != null) {
        _compressionConfigs.put(column, fieldConfig.getCompressionCodec());
      }
    }
  }

  /**
   * Text index creation info for each column is specified
   * using {@link FieldConfig} model of indicating per column
   * encoding and indexing information. Since IndexLoadingConfig
   * is created from TableConfig, we extract the text index info
   * from fieldConfigList in TableConfig.
   * @param tableConfig table config
   */
  private void extractTextIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        String column = fieldConfig.getName();
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.TEXT) {
          _textIndexColumns.add(column);
        }
      }
    }
  }

  private void extractFSTIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        String column = fieldConfig.getName();
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.FST) {
          _fstIndexColumns.add(column);
        }
      }
    }
  }

  private void extractH3IndexConfigsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.H3) {
          //noinspection ConstantConditions
          _h3IndexConfigs.put(fieldConfig.getName(), new H3IndexConfig(fieldConfig.getProperties()));
        }
      }
    }
  }

  private void extractVectorIndexConfigsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.VECTOR) {
          //noinspection ConstantConditions
          _vectorIndexConfigs.put(fieldConfig.getName(), new VectorIndexConfig(fieldConfig.getProperties()));
        }
      }
    }
  }

  private void extractFromInstanceConfig(InstanceDataManagerConfig instanceDataManagerConfig) {
    if (instanceDataManagerConfig == null) {
      return;
    }

    _instanceDataManagerConfig = instanceDataManagerConfig;
    _instanceId = instanceDataManagerConfig.getInstanceId();

    ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _isRealtimeOffHeapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffHeapAllocation = instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();

    String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.valueOf(avgMultiValueCount);
    }
    _segmentStoreURI =
        instanceDataManagerConfig.getConfig().getProperty(CommonConstants.Server.CONFIG_OF_SEGMENT_STORE_URI);
    _segmentDirectoryLoader = instanceDataManagerConfig.getSegmentDirectoryLoader();
  }

  /**
   * Forward index disabled info for each column is specified
   * using {@link FieldConfig} model of indicating per column
   * encoding and indexing information. Since IndexLoadingConfig
   * is created from TableConfig, we extract the no forward index info
   * from fieldConfigList in TableConfig via the properties bag.
   * @param tableConfig table config
   */
  private void extractForwardIndexDisabledColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        Map<String, String> fieldConfigProperties = fieldConfig.getProperties();
        if (fieldConfigProperties != null) {
          boolean forwardIndexDisabled = Boolean.parseBoolean(
              fieldConfigProperties.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED,
                  FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
          if (forwardIndexDisabled) {
            _forwardIndexDisabledColumns.add(fieldConfig.getName());
          }
        }
      }
    }
  }

  public ReadMode getReadMode() {
    return _readMode;
  }

  /**
   * For tests only.
   */
  public void setReadMode(ReadMode readMode) {
    _readMode = readMode;
    _dirty = true;
  }

  public List<String> getSortedColumns() {
    return unmodifiable(_sortedColumns);
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setSortedColumn(String sortedColumn) {
    if (sortedColumn != null) {
      _sortedColumns = new ArrayList<>();
      _sortedColumns.add(sortedColumn);
    } else {
      _sortedColumns = Collections.emptyList();
    }
    _dirty = true;
  }

  public Set<String> getInvertedIndexColumns() {
    return unmodifiable(_invertedIndexColumns);
  }

  public Set<String> getRangeIndexColumns() {
    return unmodifiable(_rangeIndexColumns);
  }

  public void addRangeIndexColumn(String... columns) {
    _rangeIndexColumns.addAll(Arrays.asList(columns));
    _dirty = true;
  }

  public int getRangeIndexVersion() {
    return _rangeIndexVersion;
  }

  public FSTType getFSTIndexType() {
    return _fstIndexType;
  }

  /**
   * Used in two places:
   * (1) In {@link PhysicalColumnIndexContainer} to create the index loading info for immutable segments
   * (2) In RealtimeSegmentDataManager to create the RealtimeSegmentConfig.
   * RealtimeSegmentConfig is used to specify the text index column info for newly
   * to-be-created Mutable Segments
   * @return a set containing names of text index columns
   */
  public Set<String> getTextIndexColumns() {
    return unmodifiable(_textIndexColumns);
  }

  public Set<String> getFSTIndexColumns() {
    return unmodifiable(_fstIndexColumns);
  }

  public Map<String, JsonIndexConfig> getJsonIndexConfigs() {
    return unmodifiable(_jsonIndexConfigs);
  }

  public Map<String, H3IndexConfig> getH3IndexConfigs() {
    return unmodifiable(_h3IndexConfigs);
  }

  public Map<String, VectorIndexConfig> getVectorIndexConfigs() {
    return unmodifiable(_vectorIndexConfigs);
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    return unmodifiable(_columnProperties);
  }

  public void setColumnProperties(Map<String, Map<String, String>> columnProperties) {
    _columnProperties = new HashMap<>(columnProperties);
    _dirty = true;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setInvertedIndexColumns(Set<String> invertedIndexColumns) {
    _invertedIndexColumns = new HashSet<>(invertedIndexColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void addInvertedIndexColumns(String... invertedIndexColumns) {
    _invertedIndexColumns.addAll(Arrays.asList(invertedIndexColumns));
    _dirty = true;
  }

  @VisibleForTesting
  public void addInvertedIndexColumns(Collection<String> invertedIndexColumns) {
    _invertedIndexColumns.addAll(invertedIndexColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void removeInvertedIndexColumns(String... invertedIndexColumns) {
    removeInvertedIndexColumns(Arrays.asList(invertedIndexColumns));
    assert _dirty;
  }

  @VisibleForTesting
  public void removeInvertedIndexColumns(Collection<String> invertedIndexColumns) {
    _invertedIndexColumns.removeAll(invertedIndexColumns);
    _dirty = true;
  }

  /**
   * For tests only.
   * Used by segmentPreProcessorTest to set raw columns.
   */
  @VisibleForTesting
  public void setNoDictionaryColumns(Set<String> noDictionaryColumns) {
    _noDictionaryColumns = new HashSet<>(noDictionaryColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void removeNoDictionaryColumns(String... noDictionaryColumns) {
    Arrays.asList(noDictionaryColumns).forEach(_noDictionaryColumns::remove);
    _dirty = true;
  }

  @VisibleForTesting
  public void removeNoDictionaryColumns(Collection<String> noDictionaryColumns) {
    noDictionaryColumns.forEach(_noDictionaryColumns::remove);
    _dirty = true;
  }

  @VisibleForTesting
  public void addNoDictionaryColumns(String... noDictionaryColumns) {
    _noDictionaryColumns.addAll(Arrays.asList(noDictionaryColumns));
    _dirty = true;
  }

  @VisibleForTesting
  public void addNoDictionaryColumns(Collection<String> noDictionaryColumns) {
    _noDictionaryColumns.addAll(noDictionaryColumns);
    _dirty = true;
  }

  /**
   * For tests only.
   * Used by segmentPreProcessorTest to set compression configs.
   */
  @VisibleForTesting
  public void setCompressionConfigs(Map<String, CompressionCodec> compressionConfigs) {
    _compressionConfigs = new HashMap<>(compressionConfigs);
    _dirty = true;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setRangeIndexColumns(Set<String> rangeIndexColumns) {
    _rangeIndexColumns = new HashSet<>(rangeIndexColumns);
    _dirty = true;
  }

  public void addRangeIndexColumns(String... rangeIndexColumns) {
    _rangeIndexColumns.addAll(Arrays.asList(rangeIndexColumns));
    _dirty = true;
  }

  public void removeRangeIndexColumns(String... rangeIndexColumns) {
    Arrays.asList(rangeIndexColumns).forEach(_rangeIndexColumns::remove);
    _dirty = true;
  }

  /**
   * Used directly from text search unit test code since the test code
   * doesn't really have a table config and is directly testing the
   * query execution code of text search using data from generated segments
   * and then loading those segments.
   */
  @VisibleForTesting
  public void setTextIndexColumns(Set<String> textIndexColumns) {
    _textIndexColumns = new HashSet<>(textIndexColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void addTextIndexColumns(String... textIndexColumns) {
    _textIndexColumns.addAll(Arrays.asList(textIndexColumns));
    _dirty = true;
  }

  @VisibleForTesting
  public void removeTextIndexColumns(String... textIndexColumns) {
    Arrays.asList(textIndexColumns).forEach(_textIndexColumns::remove);
    _dirty = true;
  }

  @VisibleForTesting
  public void setFSTIndexColumns(Set<String> fstIndexColumns) {
    _fstIndexColumns = new HashSet<>(fstIndexColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void addFSTIndexColumns(String... fstIndexColumns) {
    _fstIndexColumns.addAll(Arrays.asList(fstIndexColumns));
    _dirty = true;
  }

  @VisibleForTesting
  public void removeFSTIndexColumns(String... fstIndexColumns) {
    Arrays.asList(fstIndexColumns).forEach(_fstIndexColumns::remove);
    _dirty = true;
  }

  @VisibleForTesting
  public void setFSTIndexType(FSTType fstType) {
    _fstIndexType = fstType;
    _dirty = true;
  }

  @VisibleForTesting
  public void setJsonIndexColumns(Set<String> jsonIndexColumns) {
    if (jsonIndexColumns != null) {
      _jsonIndexConfigs = new HashMap<>();
      for (String jsonIndexColumn : jsonIndexColumns) {
        _jsonIndexConfigs.put(jsonIndexColumn, new JsonIndexConfig());
      }
    } else {
      _jsonIndexConfigs = null;
    }
    _dirty = true;
  }

  @VisibleForTesting
  public void setH3IndexConfigs(Map<String, H3IndexConfig> h3IndexConfigs) {
    _h3IndexConfigs = new HashMap<>(h3IndexConfigs);
    _dirty = true;
  }

  @VisibleForTesting
  public void setVectorIndexConfigs(Map<String, VectorIndexConfig> vectorIndexConfigs) {
    _vectorIndexConfigs = new HashMap<>(vectorIndexConfigs);
    _dirty = true;
  }

  @VisibleForTesting
  public void setBloomFilterConfigs(Map<String, BloomFilterConfig> bloomFilterConfigs) {
    _bloomFilterConfigs = new HashMap<>(bloomFilterConfigs);
    _dirty = true;
  }

  @VisibleForTesting
  public void setOnHeapDictionaryColumns(Set<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = new HashSet<>(onHeapDictionaryColumns);
    _dirty = true;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setForwardIndexDisabledColumns(Set<String> forwardIndexDisabledColumns) {
    _forwardIndexDisabledColumns =
        forwardIndexDisabledColumns == null ? new HashSet<>() : new HashSet<>(forwardIndexDisabledColumns);
    _dirty = true;
  }

  @VisibleForTesting
  public void addForwardIndexDisabledColumns(String... forwardIndexDisabledColumns) {
    _forwardIndexDisabledColumns.addAll(Arrays.asList(forwardIndexDisabledColumns));
    _dirty = true;
  }

  @VisibleForTesting
  public void removeForwardIndexDisabledColumns(String... forwardIndexDisabledColumns) {
    Arrays.asList(forwardIndexDisabledColumns).forEach(_forwardIndexDisabledColumns::remove);
    _dirty = true;
  }

  public Set<String> getNoDictionaryColumns() {
    return unmodifiable(_noDictionaryColumns);
  }

  /**
   * Populates a map containing column name as key and compression type as value. This map will only contain the
   * compressionType overrides, and it does not correspond to the default value of compressionType (derived using
   * SegmentColumnarIndexCreator.getColumnCompressionType())  used for a column. Note that only RAW forward index
   * columns will be populated in this map.
   *
   * @return a map containing column name as key and compressionType as value.
   */
  public Map<String, CompressionCodec> getCompressionConfigs() {
    return unmodifiable(_compressionConfigs);
  }

  public Map<String, String> getNoDictionaryConfig() {
    return unmodifiable(_noDictionaryConfig);
  }

  public Set<String> getVarLengthDictionaryColumns() {
    return unmodifiable(_varLengthDictionaryColumns);
  }

  public Set<String> getOnHeapDictionaryColumns() {
    return unmodifiable(_onHeapDictionaryColumns);
  }

  public Set<String> getForwardIndexDisabledColumns() {
    return unmodifiable(_forwardIndexDisabledColumns);
  }

  public Map<String, BloomFilterConfig> getBloomFilterConfigs() {
    return unmodifiable(_bloomFilterConfigs);
  }

  public Map<String, OnHeapDictionaryConfig> getOnHeapDictionaryConfigs() {
    return unmodifiable(_onHeapDictionaryConfigs);
  }

  public boolean isEnableDynamicStarTreeCreation() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDynamicStarTreeCreation;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_starTreeIndexConfigs);
  }

  public boolean isEnableDefaultStarTree() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDefaultStarTree;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  /**
   * For tests only.
   */
  public void setSegmentVersion(SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
    _dirty = true;
  }

  public boolean isRealtimeOffHeapAllocation() {
    return _isRealtimeOffHeapAllocation;
  }

  public boolean isDirectRealtimeOffHeapAllocation() {
    return _isDirectRealtimeOffHeapAllocation;
  }

  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
  }

  public String getSegmentStoreURI() {
    return _segmentStoreURI;
  }

  /**
   * For tests only.
   */
  public void setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
    _dirty = true;
  }

  public int getRealtimeAvgMultiValueCount() {
    return _realtimeAvgMultiValueCount;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _schema;
  }

  @VisibleForTesting
  public void setTableConfig(TableConfig tableConfig) {
    _tableConfig = tableConfig;
    _dirty = true;
  }

  public String getSegmentDirectoryLoader() {
    return StringUtils.isNotBlank(_segmentDirectoryLoader) ? _segmentDirectoryLoader
        : SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(READ_MODE_KEY, _readMode);
    return new PinotConfiguration(props);
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public void setTableDataDir(String tableDataDir) {
    _tableDataDir = tableDataDir;
    _dirty = true;
  }

  public boolean isErrorOnColumnBuildFailure() {
    return _errorOnColumnBuildFailure;
  }

  public void setErrorOnColumnBuildFailure(boolean errorOnColumnBuildFailure) {
    _errorOnColumnBuildFailure = errorOnColumnBuildFailure;
  }

  public String getTableDataDir() {
    return _tableDataDir;
  }

  public void setSegmentTier(String segmentTier) {
    _segmentTier = segmentTier;
    _dirty = true;
  }

  public String getSegmentTier() {
    return _segmentTier;
  }

  @Nullable
  public FieldIndexConfigs getFieldIndexConfig(String columnName) {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return _indexConfigsByColName.get(columnName);
  }

  public Map<String, FieldIndexConfigs> getFieldIndexConfigByColName() {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_indexConfigsByColName);
  }

  /**
   * Returns a subset of the columns on the table.
   *
   * When {@link #getSchema()} is defined, the subset is equal the columns on the schema. In other cases, this method
   * tries its bests to get the columns from other attributes like {@link #getTableConfig()}, which may also not be
   * defined or may not be complete.
   */
  public Set<String> getAllKnownColumns() {
    if (_schema != null) {
      return _schema.getColumnNames();
    }
    if (!_dirty && _knownColumns != null) {
      return _knownColumns;
    }
    if (_knownColumns == null) {
      _knownColumns = new HashSet<>();
    }
    if (_tableConfig != null) {
      List<FieldConfig> fieldConfigs = _tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          _knownColumns.add(fieldConfig.getName());
        }
      }
    }
    _knownColumns.addAll(_columnProperties.keySet());
    _knownColumns.addAll(_invertedIndexColumns);
    _knownColumns.addAll(_fstIndexColumns);
    _knownColumns.addAll(_rangeIndexColumns);
    _knownColumns.addAll(_noDictionaryColumns);
    _knownColumns.addAll(_textIndexColumns);
    _knownColumns.addAll(_forwardIndexDisabledColumns);
    _knownColumns.addAll(_onHeapDictionaryColumns);
    _knownColumns.addAll(_varLengthDictionaryColumns);
    return _knownColumns;
  }

  public void setInstanceTierConfigs(Map<String, Map<String, String>> tierConfigs) {
    _instanceTierConfigs = new HashMap<>(tierConfigs);
    _dirty = true;
  }

  public Map<String, Map<String, String>> getInstanceTierConfigs() {
    return unmodifiable(_instanceTierConfigs);
  }

  private <E> List<E> unmodifiable(List<E> list) {
    return list == null ? null : Collections.unmodifiableList(list);
  }

  private <E> Set<E> unmodifiable(Set<E> set) {
    return set == null ? null : Collections.unmodifiableSet(set);
  }

  private <K, V> Map<K, V> unmodifiable(Map<K, V> map) {
    return map == null ? null : Collections.unmodifiableMap(map);
  }

  public void addKnownColumns(Set<String> columns) {
    if (_knownColumns == null) {
      _knownColumns = new HashSet<>(columns);
    } else {
      _knownColumns.addAll(columns);
    }
    _dirty = true;
  }
}
