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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;
  public static final String READ_MODE_KEY = "readMode";

  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private List<String> _sortedColumns = Collections.emptyList();
  private Set<String> _invertedIndexColumns = new HashSet<>();
  private Set<String> _rangeIndexColumns = new HashSet<>();
  private int _rangeIndexVersion = IndexingConfig.DEFAULT_RANGE_INDEX_VERSION;
  private Set<String> _textIndexColumns = new HashSet<>();
  private Set<String> _fstIndexColumns = new HashSet<>();
  private FSTType _fstIndexType = FSTType.LUCENE;
  private Map<String, JsonIndexConfig> _jsonIndexConfigs = new HashMap<>();
  private Map<String, H3IndexConfig> _h3IndexConfigs = new HashMap<>();
  private Set<String> _noDictionaryColumns = new HashSet<>(); // TODO: replace this by _noDictionaryConfig.
  private final Map<String, String> _noDictionaryConfig = new HashMap<>();
  private final Set<String> _varLengthDictionaryColumns = new HashSet<>();
  private Set<String> _onHeapDictionaryColumns = new HashSet<>();
  private Set<String> _forwardIndexDisabledColumns = new HashSet<>();
  private Map<String, BloomFilterConfig> _bloomFilterConfigs = new HashMap<>();
  private boolean _enableDynamicStarTreeCreation;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private boolean _enableDefaultStarTree;
  private Map<String, ChunkCompressionType> _compressionConfigs = new HashMap<>();

  private SegmentVersion _segmentVersion;
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private boolean _enableSplitCommit;
  private boolean _isRealtimeOffHeapAllocation;
  private boolean _isDirectRealtimeOffHeapAllocation;
  private boolean _enableSplitCommitEndWithMetadata;
  private String _segmentStoreURI;

  // constructed from FieldConfig
  private Map<String, Map<String, String>> _columnProperties = new HashMap<>();

  private TableConfig _tableConfig;
  private String _tableDataDir;
  private String _segmentDirectoryLoader;
  private String _segmentTier;

  private String _instanceId;

  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    extractFromInstanceConfig(instanceDataManagerConfig);
    extractFromTableConfig(tableConfig);
    _tableConfig = tableConfig;
  }

  private void extractFromTableConfig(TableConfig tableConfig) {
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
    extractForwardIndexDisabledColumnsFromTableConfig(tableConfig);

    Map<String, List<TimestampIndexGranularity>> timestampIndexConfigs =
        SegmentGeneratorConfig.extractTimestampIndexConfigsFromTableConfig(tableConfig);
    if (!timestampIndexConfigs.isEmpty()) {
      // Apply transform function and range index to the timestamp with granularity columns
      IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
      if (ingestionConfig == null) {
        ingestionConfig = new IngestionConfig();
        tableConfig.setIngestionConfig(ingestionConfig);
      }
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs == null) {
        transformConfigs = new ArrayList<>();
        ingestionConfig.setTransformConfigs(transformConfigs);
      }
      for (Map.Entry<String, List<TimestampIndexGranularity>> entry : timestampIndexConfigs.entrySet()) {
        String column = entry.getKey();
        for (TimestampIndexGranularity granularity : entry.getValue()) {
          String columnNameWithGranularity =
              TimestampIndexGranularity.getColumnNameWithGranularity(column, granularity);
          TransformConfig transformConfig = new TransformConfig(columnNameWithGranularity,
              TimestampIndexGranularity.getTransformExpression(column, granularity));
          transformConfigs.add(transformConfig);
          _rangeIndexColumns.add(columnNameWithGranularity);
        }
      }
    }

    Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
    if (noDictionaryConfig != null) {
      _noDictionaryConfig.putAll(noDictionaryConfig);
    }

    List<String> varLengthDictionaryColumns = indexingConfig.getVarLengthDictionaryColumns();
    if (varLengthDictionaryColumns != null) {
      _varLengthDictionaryColumns.addAll(varLengthDictionaryColumns);
    }

    List<String> onHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns();
    if (onHeapDictionaryColumns != null) {
      _onHeapDictionaryColumns.addAll(onHeapDictionaryColumns);
    }

    _enableDynamicStarTreeCreation = indexingConfig.isEnableDynamicStarTreeCreation();
    _starTreeIndexConfigs = indexingConfig.getStarTreeIndexConfigs();
    _enableDefaultStarTree = indexingConfig.isEnableDefaultStarTree();

    String tableSegmentVersion = indexingConfig.getSegmentFormatVersion();
    if (tableSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
    }

    String columnMinMaxValueGeneratorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode != null) {
      _columnMinMaxValueGeneratorMode =
          ColumnMinMaxValueGeneratorMode.valueOf(columnMinMaxValueGeneratorMode.toUpperCase());
    }
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
        ChunkCompressionType compressionType = ChunkCompressionType.valueOf(fieldConfig.getCompressionCodec().name());
        _compressionConfigs.put(column, compressionType);
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
          Map<String, String> propertiesMap = fieldConfig.getProperties();
          if (TextIndexUtils.isFstTypeNative(propertiesMap)) {
            _fstIndexType = FSTType.NATIVE;
          }
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

  private void extractFromInstanceConfig(InstanceDataManagerConfig instanceDataManagerConfig) {
    if (instanceDataManagerConfig == null) {
      return;
    }
    _instanceId = instanceDataManagerConfig.getInstanceId();

    ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _enableSplitCommit = instanceDataManagerConfig.isEnableSplitCommit();

    _isRealtimeOffHeapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffHeapAllocation = instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();

    String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.valueOf(avgMultiValueCount);
    }
    _enableSplitCommitEndWithMetadata = instanceDataManagerConfig.isEnableSplitCommitEndWithMetadata();
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
          boolean forwardIndexDisabled = Boolean.parseBoolean(fieldConfigProperties
              .getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
          if (forwardIndexDisabled) {
            _forwardIndexDisabledColumns.add(fieldConfig.getName());
          }
        }
      }
    }
  }

  /**
   * For tests only.
   */
  public IndexLoadingConfig() {
  }

  public ReadMode getReadMode() {
    return _readMode;
  }

  /**
   * For tests only.
   */
  public void setReadMode(ReadMode readMode) {
    _readMode = readMode;
  }

  public List<String> getSortedColumns() {
    return _sortedColumns;
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
  }

  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public Set<String> getRangeIndexColumns() {
    return _rangeIndexColumns;
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
   * (2) In LLRealtimeSegmentDataManager to create the RealtimeSegmentConfig.
   * RealtimeSegmentConfig is used to specify the text index column info for newly
   * to-be-created Mutable Segments
   * @return a set containing names of text index columns
   */
  public Set<String> getTextIndexColumns() {
    return _textIndexColumns;
  }

  public Set<String> getFSTIndexColumns() {
    return _fstIndexColumns;
  }

  public Map<String, JsonIndexConfig> getJsonIndexConfigs() {
    return _jsonIndexConfigs;
  }

  public Map<String, H3IndexConfig> getH3IndexConfigs() {
    return _h3IndexConfigs;
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    return _columnProperties;
  }

  public void setColumnProperties(Map<String, Map<String, String>> columnProperties) {
    _columnProperties = columnProperties;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setInvertedIndexColumns(Set<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
  }

  /**
   * For tests only.
   * Used by segmentPreProcessorTest to set raw columns.
   */
  @VisibleForTesting
  public void setNoDictionaryColumns(Set<String> noDictionaryColumns) {
    _noDictionaryColumns = noDictionaryColumns;
  }

  /**
   * For tests only.
   * Used by segmentPreProcessorTest to set compression configs.
   */
  @VisibleForTesting
  public void setCompressionConfigs(Map<String, ChunkCompressionType> compressionConfigs) {
    _compressionConfigs = compressionConfigs;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setRangeIndexColumns(Set<String> rangeIndexColumns) {
    _rangeIndexColumns = rangeIndexColumns;
  }

  /**
   * Used directly from text search unit test code since the test code
   * doesn't really have a table config and is directly testing the
   * query execution code of text search using data from generated segments
   * and then loading those segments.
   */
  @VisibleForTesting
  public void setTextIndexColumns(Set<String> textIndexColumns) {
    _textIndexColumns = textIndexColumns;
  }

  @VisibleForTesting
  public void setFSTIndexColumns(Set<String> fstIndexColumns) {
    _fstIndexColumns = fstIndexColumns;
  }

  @VisibleForTesting
  public void setFSTIndexType(FSTType fstType) {
    _fstIndexType = fstType;
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
  }

  @VisibleForTesting
  public void setH3IndexConfigs(Map<String, H3IndexConfig> h3IndexConfigs) {
    _h3IndexConfigs = h3IndexConfigs;
  }

  @VisibleForTesting
  public void setBloomFilterConfigs(Map<String, BloomFilterConfig> bloomFilterConfigs) {
    _bloomFilterConfigs = bloomFilterConfigs;
  }

  @VisibleForTesting
  public void setOnHeapDictionaryColumns(Set<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setForwardIndexDisabledColumns(Set<String> forwardIndexDisabledColumns) {
    _forwardIndexDisabledColumns = forwardIndexDisabledColumns;
  }

  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  /**
   * Populates a map containing column name as key and compression type as value. This map will only contain the
   * compressionType overrides, and it does not correspond to the default value of compressionType (derived using
   * SegmentColumnarIndexCreator.getColumnCompressionType())  used for a column. Note that only RAW forward index
   * columns will be populated in this map.
   *
   * @return a map containing column name as key and compressionType as value.
   */
  public Map<String, ChunkCompressionType> getCompressionConfigs() {
    return _compressionConfigs;
  }

  public Map<String, String> getNoDictionaryConfig() {
    return _noDictionaryConfig;
  }

  public Set<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  public Set<String> getOnHeapDictionaryColumns() {
    return _onHeapDictionaryColumns;
  }

  public Set<String> getForwardIndexDisabledColumns() {
    return _forwardIndexDisabledColumns;
  }

  public Map<String, BloomFilterConfig> getBloomFilterConfigs() {
    return _bloomFilterConfigs;
  }

  public boolean isEnableDynamicStarTreeCreation() {
    return _enableDynamicStarTreeCreation;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    return _starTreeIndexConfigs;
  }

  public boolean isEnableDefaultStarTree() {
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
  }

  public boolean isEnableSplitCommit() {
    return _enableSplitCommit;
  }

  public boolean isEnableSplitCommitEndWithMetadata() {
    return _enableSplitCommitEndWithMetadata;
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
  }

  public int getRealtimeAvgMultiValueCount() {
    return _realtimeAvgMultiValueCount;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @VisibleForTesting
  public void setTableConfig(TableConfig tableConfig) {
    _tableConfig = tableConfig;
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
  }

  public String getTableDataDir() {
    return _tableDataDir;
  }

  public void setSegmentTier(String segmentTier) {
    _segmentTier = segmentTier;
  }

  public String getSegmentTier() {
    return _segmentTier;
  }
}
