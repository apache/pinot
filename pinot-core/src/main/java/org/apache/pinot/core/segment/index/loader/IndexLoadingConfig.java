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
package org.apache.pinot.core.segment.index.loader;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.manager.config.InstanceDataManagerConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;

  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private List<String> _sortedColumns = Collections.emptyList();
  private Set<String> _invertedIndexColumns = new HashSet<>();
  private Set<String> _textIndexColumns = new HashSet<>();
  private Set<String> _rangeIndexColumns = new HashSet<>();
  private Set<String> _noDictionaryColumns = new HashSet<>(); // TODO: replace this by _noDictionaryConfig.
  private Map<String, String> _noDictionaryConfig = new HashMap<>();
  private Set<String> _varLengthDictionaryColumns = new HashSet<>();
  private Set<String> _onHeapDictionaryColumns = new HashSet<>();
  private Set<String> _bloomFilterColumns = new HashSet<>();

  private SegmentVersion _segmentVersion;
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private boolean _enableSplitCommit;
  private boolean _isRealtimeOffheapAllocation;
  private boolean _isDirectRealtimeOffheapAllocation;
  private boolean _enableSplitCommitEndWithMetadata;

  // constructed from FieldConfig
  private Map<String, Map<String, String>> _columnProperties = new HashMap<>();

  public IndexLoadingConfig(@Nonnull InstanceDataManagerConfig instanceDataManagerConfig,
      @Nonnull TableConfig tableConfig) {
    extractFromInstanceConfig(instanceDataManagerConfig);
    extractFromTableConfig(tableConfig);
  }

  private void extractFromTableConfig(@Nonnull TableConfig tableConfig) {
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

    List<String> rangeIndexColumns = indexingConfig.getRangeIndexColumns();
    if (rangeIndexColumns != null) {
      _rangeIndexColumns.addAll(rangeIndexColumns);
    }

    List<String> bloomFilterColumns = indexingConfig.getBloomFilterColumns();
    if (bloomFilterColumns != null) {
      _bloomFilterColumns.addAll(bloomFilterColumns);
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

    extractTextIndexColumnsFromTableConfig(tableConfig);

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
          if (fieldConfig.getEncodingType() != FieldConfig.EncodingType.RAW || !_noDictionaryColumns.contains(column)) {
            throw new UnsupportedOperationException("Text index is currently not supported on dictionary encoded column: " + column);
          }
          _textIndexColumns.add(column);
        }
      }
    }
  }

  private void extractFromInstanceConfig(@Nonnull InstanceDataManagerConfig instanceDataManagerConfig) {
    ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _enableSplitCommit = instanceDataManagerConfig.isEnableSplitCommit();

    _isRealtimeOffheapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffheapAllocation = instanceDataManagerConfig.isDirectRealtimeOffheapAllocation();

    String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.valueOf(avgMultiValueCount);
    }
    _enableSplitCommitEndWithMetadata = instanceDataManagerConfig.isEnableSplitCommitEndWithMetadata();
  }

  /**
   * For tests only.
   */
  public IndexLoadingConfig() {
  }

  @Nonnull
  public ReadMode getReadMode() {
    return _readMode;
  }

  /**
   * For tests only.
   */
  public void setReadMode(@Nonnull ReadMode readMode) {
    _readMode = readMode;
  }

  @Nonnull
  public List<String> getSortedColumns() {
    return _sortedColumns;
  }

  @Nonnull
  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }  @Nonnull

  public Set<String> getRangeIndexColumns() {
    return _rangeIndexColumns;
  }

  @Nonnull
  public Map<String, Map<String, String>> getColumnProperties() {
    return _columnProperties;
  }

  /**
   * Used in two places:
   * (1) In {@link org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer}
   * to create the index loading info for immutable segments
   * (2) In {@link org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager}
   * to create the {@link org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig}.
   * RealtimeSegmentConfig is used to specify the text index column info for newly
   * to-be-created Mutable Segments
   * @return a set containing names of text index columns
   */
  @Nonnull
  public Set<String> getTextIndexColumns() {
    return _textIndexColumns;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setInvertedIndexColumns(@Nonnull Set<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setRangeIndexColumns(@Nonnull Set<String> rangeIndexColumns) {
    _rangeIndexColumns = rangeIndexColumns;
  }

  /**
   * Used directly from text search unit test code since the test code
   * doesn't really have a table config and is directly testing the
   * query execution code of text search using data from generated segments
   * and then loading those segments.
   */
  @VisibleForTesting
  public void setTextIndexColumns(@Nonnull Set<String> textIndexColumns) {
    _textIndexColumns = textIndexColumns;
  }

  @VisibleForTesting
  public void setBloomFilterColumns(@Nonnull Set<String> bloomFilterColumns) {
    _bloomFilterColumns = bloomFilterColumns;
  }

  @VisibleForTesting
  public void setOnHeapDictionaryColumns(@Nonnull Set<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
  }

  @Nonnull
  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  @Nonnull
  public Map<String, String> getnoDictionaryConfig() {
    return _noDictionaryConfig;
  }

  @Nonnull
  public Set<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  @Nonnull
  public Set<String> getOnHeapDictionaryColumns() {
    return _onHeapDictionaryColumns;
  }

  public Set<String> getBloomFilterColumns() {
    return _bloomFilterColumns;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  /**
   * For tests only.
   */
  public void setSegmentVersion(@Nonnull SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
  }

  public boolean isEnableSplitCommit() {
    return _enableSplitCommit;
  }

  public boolean isEnableSplitCommitEndWithMetadata() {
    return _enableSplitCommitEndWithMetadata;
  }

  public boolean isRealtimeOffheapAllocation() {
    return _isRealtimeOffheapAllocation;
  }

  public boolean isDirectRealtimeOffheapAllocation() {
    return _isDirectRealtimeOffheapAllocation;
  }

  @Nonnull
  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
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
}
