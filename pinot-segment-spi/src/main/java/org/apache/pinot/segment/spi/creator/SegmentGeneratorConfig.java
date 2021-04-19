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
package org.apache.pinot.segment.spi.creator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration properties used in the creation of index segments.
 */
public class SegmentGeneratorConfig implements Serializable {
  public enum TimeColumnType {
    EPOCH, SIMPLE_DATE
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGeneratorConfig.class);

  private TableConfig _tableConfig;
  private final Map<String, String> _customProperties = new HashMap<>();
  private final Set<String> _rawIndexCreationColumns = new HashSet<>();
  private final Map<String, ChunkCompressionType> _rawIndexCompressionType = new HashMap<>();
  private final List<String> _invertedIndexCreationColumns = new ArrayList<>();
  private final List<String> _textIndexCreationColumns = new ArrayList<>();
  private final List<String> _fstIndexCreationColumns = new ArrayList<>();
  private final List<String> _jsonIndexCreationColumns = new ArrayList<>();
  private final Map<String, H3IndexConfig> _h3IndexConfigs = new HashMap<>();
  private final List<String> _columnSortOrder = new ArrayList<>();
  private List<String> _varLengthDictionaryColumns = new ArrayList<>();
  private String _inputFilePath = null;
  private FileFormat _format = FileFormat.AVRO;
  private String _recordReaderPath = null; //TODO: this should be renamed to recordReaderClass or even better removed
  private String _outDir = null;
  private String _rawTableName = null;
  private String _segmentName = null;
  private String _segmentNamePostfix = null;
  private String _segmentTimeColumnName = null;
  private TimeUnit _segmentTimeUnit = null;
  private String _segmentCreationTime = null;
  private String _segmentStartTime = null;
  private String _segmentEndTime = null;
  private SegmentVersion _segmentVersion = SegmentVersion.v3;
  private Schema _schema = null;
  private RecordReaderConfig _readerConfig = null;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs = null;
  private boolean _enableDefaultStarTree = false;
  private String _creatorVersion = null;
  private SegmentNameGenerator _segmentNameGenerator = null;
  private SegmentPartitionConfig _segmentPartitionConfig = null;
  private int _sequenceId = -1;
  private TimeColumnType _timeColumnType = TimeColumnType.EPOCH;
  private String _simpleDateFormat = null;
  // Use on-heap or off-heap memory to generate index (currently only affect inverted index and star-tree v2)
  private boolean _onHeap = false;
  private boolean _skipTimeValueCheck = false;
  private boolean _nullHandlingEnabled = false;
  private boolean _failOnEmptySegment = false;

  // constructed from FieldConfig
  private Map<String, Map<String, String>> _columnProperties = new HashMap<>();

  @Deprecated
  public SegmentGeneratorConfig() {
  }

  /**
   * Construct the SegmentGeneratorConfig using schema and table config.
   * If table config is passed, it will be used to initialize the time column details and the indexing config
   * This constructor is used during offline data generation.
   * @param tableConfig table config of the segment. Used for getting time column information and indexing information
   * @param schema schema of the segment to be generated. The time column information should be taken from table config.
   *               However, for maintaining backward compatibility, taking it from schema if table config is null.
   *               This will not work once we start supporting multiple time columns (DateTimeFieldSpec)
   */
  public SegmentGeneratorConfig(TableConfig tableConfig, Schema schema) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(tableConfig);
    setSchema(schema);

    _tableConfig = tableConfig;
    setTableName(tableConfig.getTableName());

    // NOTE: SegmentGeneratorConfig#setSchema doesn't set the time column anymore. timeColumnName is expected to be read from table config.
    String timeColumnName = null;
    if (tableConfig.getValidationConfig() != null) {
      timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    }
    setTime(timeColumnName, schema);

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null) {
      String segmentVersion = indexingConfig.getSegmentFormatVersion();
      if (segmentVersion != null) {
        _segmentVersion = SegmentVersion.valueOf(segmentVersion);
      }

      List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
      Map<String, String> noDictionaryColumnMap = indexingConfig.getNoDictionaryConfig();

      if (noDictionaryColumns != null) {
        this.setRawIndexCreationColumns(noDictionaryColumns);

        if (noDictionaryColumnMap != null) {
          Map<String, ChunkCompressionType> serializedNoDictionaryColumnMap =
              noDictionaryColumnMap.entrySet().stream().collect(Collectors
                  .toMap(Map.Entry::getKey, e -> ChunkCompressionType.valueOf(e.getValue())));
          this.setRawIndexCompressionType(serializedNoDictionaryColumnMap);
        }
      }
      if (indexingConfig.getVarLengthDictionaryColumns() != null) {
        setVarLengthDictionaryColumns(indexingConfig.getVarLengthDictionaryColumns());
      }
      _segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();

      // Star-tree configs
      setStarTreeIndexConfigs(indexingConfig.getStarTreeIndexConfigs());
      setEnableDefaultStarTree(indexingConfig.isEnableDefaultStarTree());

      // NOTE: There are 2 ways to configure creating inverted index during segment generation:
      //       - Set 'generate.inverted.index.before.push' to 'true' in custom config (deprecated)
      //       - Enable 'createInvertedIndexDuringSegmentGeneration' in indexing config
      // TODO: Clean up the table configs with the deprecated settings, and always use the one in the indexing config
      if (indexingConfig.getInvertedIndexColumns() != null) {
        Map<String, String> customConfigs = tableConfig.getCustomConfig().getCustomConfigs();
        if ((customConfigs != null && Boolean.parseBoolean(customConfigs.get("generate.inverted.index.before.push")))
            || indexingConfig.isCreateInvertedIndexDuringSegmentGeneration()) {
          _invertedIndexCreationColumns.addAll(indexingConfig.getInvertedIndexColumns());
        }
      }

      if (indexingConfig.getJsonIndexColumns() != null) {
        _jsonIndexCreationColumns.addAll(indexingConfig.getJsonIndexColumns());
      }

      List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
      if (fieldConfigList != null) {
        for (FieldConfig fieldConfig : fieldConfigList) {
          _columnProperties.put(fieldConfig.getName(), fieldConfig.getProperties());
        }
      }

      extractTextIndexColumnsFromTableConfig(tableConfig);
      extractFSTIndexColumnsFromTableConfig(tableConfig);
      extractH3IndexConfigsFromTableConfig(tableConfig);

      _nullHandlingEnabled = indexingConfig.isNullHandlingEnabled();
    }
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    return _columnProperties;
  }

  /**
   * Set time column details using the given time column
   */
  private void setTime(@Nullable String timeColumnName, Schema schema) {
    if (timeColumnName != null) {
      DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
      if (dateTimeFieldSpec != null) {
        setTimeColumnName(dateTimeFieldSpec.getName());
        DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
        if (formatSpec.getTimeFormat().equals(DateTimeFieldSpec.TimeFormat.EPOCH)) {
          setSegmentTimeUnit(formatSpec.getColumnUnit());
        } else {
          setSimpleDateFormat(formatSpec.getSDFPattern());
        }
      }
    }
  }

  /**
   * Text index creation info for each column is specified
   * using {@link FieldConfig} model of indicating per column
   * encoding and indexing information. Since SegmentGeneratorConfig
   * is created from TableConfig, we extract the text index info
   * from fieldConfigList in TableConfig.
   * @param tableConfig table config
   */
  private void extractTextIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.TEXT) {
          _textIndexCreationColumns.add(fieldConfig.getName());
        }
      }
    }
  }

  private void extractFSTIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.FST) {
          _fstIndexCreationColumns.add(fieldConfig.getName());
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

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public void setCustomProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    _customProperties.putAll(properties);
  }

  public void setSimpleDateFormat(String simpleDateFormat) {
    _timeColumnType = TimeColumnType.SIMPLE_DATE;
    try {
      DateTimeFormat.forPattern(simpleDateFormat);
    } catch (Exception e) {
      throw new RuntimeException("Illegal simple date format specification", e);
    }
    _simpleDateFormat = simpleDateFormat;
  }

  public String getSimpleDateFormat() {
    return _simpleDateFormat;
  }

  public TimeColumnType getTimeColumnType() {
    return _timeColumnType;
  }

  public boolean containsCustomProperty(String key) {
    Preconditions.checkNotNull(key);
    return _customProperties.containsKey(key);
  }

  public Set<String> getRawIndexCreationColumns() {
    return _rawIndexCreationColumns;
  }

  public List<String> getInvertedIndexCreationColumns() {
    return _invertedIndexCreationColumns;
  }

  /**
   * Used by org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator
   * to get the list of text index columns.
   * @return list of text index columns.
   */
  public List<String> getTextIndexCreationColumns() {
    return _textIndexCreationColumns;
  }

  public List<String> getFSTIndexCreationColumns() {
    return _fstIndexCreationColumns;
  }

  public List<String> getJsonIndexCreationColumns() {
    return _jsonIndexCreationColumns;
  }

  public Map<String, H3IndexConfig> getH3IndexConfigs() {
    return _h3IndexConfigs;
  }

  public List<String> getColumnSortOrder() {
    return _columnSortOrder;
  }

  public void setRawIndexCreationColumns(List<String> rawIndexCreationColumns) {
    Preconditions.checkNotNull(rawIndexCreationColumns);
    _rawIndexCreationColumns.addAll(rawIndexCreationColumns);
  }

  // NOTE: Should always be extracted from the table config
  @Deprecated
  public void setInvertedIndexCreationColumns(List<String> indexCreationColumns) {
    Preconditions.checkNotNull(indexCreationColumns);
    _invertedIndexCreationColumns.addAll(indexCreationColumns);
  }

  /**
   * Used by org.apache.pinot.core.realtime.converter.RealtimeSegmentConverter
   * and text search functional tests
   * @param textIndexCreationColumns list of columns with text index creation enabled
   */
  public void setTextIndexCreationColumns(List<String> textIndexCreationColumns) {
    if (textIndexCreationColumns != null) {
      _textIndexCreationColumns.addAll(textIndexCreationColumns);
    }
  }

  @VisibleForTesting
  public void setColumnProperties(Map<String, Map<String, String>> columnProperties) {
    _columnProperties = columnProperties;
  }

  public void setFSTIndexCreationColumns(List<String> fstIndexCreationColumns) {
    if (fstIndexCreationColumns != null) {
      _fstIndexCreationColumns.addAll(fstIndexCreationColumns);
    }
  }

  public void setColumnSortOrder(List<String> sortOrder) {
    Preconditions.checkNotNull(sortOrder);
    _columnSortOrder.addAll(sortOrder);
  }

  public List<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  public void setVarLengthDictionaryColumns(List<String> varLengthDictionaryColumns) {
    this._varLengthDictionaryColumns = varLengthDictionaryColumns;
  }

  public void createInvertedIndexForColumn(String column) {
    Preconditions.checkNotNull(column);
    if (_schema != null && _schema.getFieldSpecFor(column) == null) {
      LOGGER.warn("Cannot find column {} in schema, will not create inverted index.", column);
      return;
    }
    if (_schema == null) {
      LOGGER.warn("Schema has not been set, column {} might not exist in schema after all.", column);
    }
    _invertedIndexCreationColumns.add(column);
  }

  public void createInvertedIndexForAllColumns() {
    if (_schema == null) {
      LOGGER.warn("Schema has not been set, will not create inverted index for all columns.");
      return;
    }
    for (FieldSpec spec : _schema.getAllFieldSpecs()) {
      _invertedIndexCreationColumns.add(spec.getName());
    }
  }

  public String getInputFilePath() {
    return _inputFilePath;
  }

  public void setInputFilePath(String inputFilePath) {
    Preconditions.checkNotNull(inputFilePath);
    File inputFile = new File(inputFilePath);
    Preconditions.checkState(inputFile.exists(), "Input path {} does not exist.", inputFilePath);
    _inputFilePath = inputFile.getAbsolutePath();
  }

  public FileFormat getFormat() {
    return _format;
  }

  public void setFormat(FileFormat format) {
    _format = format;
  }

  public String getRecordReaderPath() {
    return _recordReaderPath;
  }

  public void setRecordReaderPath(String recordReaderPath) {
    _recordReaderPath = recordReaderPath;
  }

  public String getOutDir() {
    return _outDir;
  }

  public void setOutDir(String dir) {
    Preconditions.checkNotNull(dir);
    File outputDir = new File(dir);
    if (outputDir.exists()) {
      Preconditions.checkState(outputDir.isDirectory(), "Path: %s is not a directory", dir);
    } else {
      Preconditions.checkState(outputDir.mkdirs(), "Cannot create output dir: %s", dir);
    }
    _outDir = outputDir.getAbsolutePath();
  }

  public String getTableName() {
    return _rawTableName;
  }

  public void setTableName(String tableName) {
    _rawTableName = tableName != null ? TableNameBuilder.extractRawTableName(tableName) : null;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getCreatorVersion() {
    return _creatorVersion;
  }

  public void setCreatorVersion(String creatorVersion) {
    _creatorVersion = creatorVersion;
  }

  public String getSegmentNamePostfix() {
    return _segmentNamePostfix;
  }

  /**
   * If you are adding a sequence Id to the segment, please use setSequenceId.
   */
  public void setSegmentNamePostfix(String postfix) {
    _segmentNamePostfix = postfix;
  }

  public String getTimeColumnName() {
    return _segmentTimeColumnName;
  }

  public void setTimeColumnName(String timeColumnName) {
    _segmentTimeColumnName = timeColumnName;
  }

  public int getSequenceId() {
    return _sequenceId;
  }

  /**
   * This method should be used instead of setPostfix if you are adding a sequence number.
   */
  public void setSequenceId(int sequenceId) {
    _sequenceId = sequenceId;
  }

  public TimeUnit getSegmentTimeUnit() {
    return _segmentTimeUnit;
  }

  public void setSegmentTimeUnit(TimeUnit timeUnit) {
    _segmentTimeUnit = timeUnit;
  }

  public String getCreationTime() {
    return _segmentCreationTime;
  }

  public void setCreationTime(String creationTime) {
    _segmentCreationTime = creationTime;
  }

  public String getStartTime() {
    return _segmentStartTime;
  }

  public void setStartTime(String startTime) {
    _segmentStartTime = startTime;
  }

  public String getEndTime() {
    return _segmentEndTime;
  }

  public void setEndTime(String endTime) {
    _segmentEndTime = endTime;
  }

  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  public void setSegmentVersion(SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
  }

  public Schema getSchema() {
    return _schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  private void setSchema(Schema schema) {
    Preconditions.checkNotNull(schema);
    _schema = schema;

    // Remove inverted index columns not in schema
    // TODO: add a validate() method to perform all validations
    if (_invertedIndexCreationColumns != null) {
      Iterator<String> iterator = _invertedIndexCreationColumns.iterator();
      while (iterator.hasNext()) {
        String column = iterator.next();
        if (_schema.getFieldSpecFor(column) == null) {
          LOGGER.warn("Cannot find column {} in schema, will not create inverted index.", column);
          iterator.remove();
        }
      }
    }
  }

  public RecordReaderConfig getReaderConfig() {
    return _readerConfig;
  }

  public void setReaderConfig(RecordReaderConfig readerConfig) {
    _readerConfig = readerConfig;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    return _starTreeIndexConfigs;
  }

  public void setStarTreeIndexConfigs(List<StarTreeIndexConfig> starTreeIndexConfigs) {
    _starTreeIndexConfigs = starTreeIndexConfigs;
  }

  public boolean isEnableDefaultStarTree() {
    return _enableDefaultStarTree;
  }

  public void setEnableDefaultStarTree(boolean enableDefaultStarTree) {
    _enableDefaultStarTree = enableDefaultStarTree;
  }

  public SegmentNameGenerator getSegmentNameGenerator() {
    if (_segmentNameGenerator != null) {
      return _segmentNameGenerator;
    }
    if (_segmentName != null) {
      return new FixedSegmentNameGenerator(_segmentName);
    }
    return new SimpleSegmentNameGenerator(_rawTableName, _segmentNamePostfix);
  }

  public void setSegmentNameGenerator(SegmentNameGenerator segmentNameGenerator) {
    _segmentNameGenerator = segmentNameGenerator;
  }

  public boolean isOnHeap() {
    return _onHeap;
  }

  public void setOnHeap(boolean onHeap) {
    _onHeap = onHeap;
  }

  public boolean isSkipTimeValueCheck() {
    return _skipTimeValueCheck;
  }

  public void setSkipTimeValueCheck(boolean skipTimeValueCheck) {
    _skipTimeValueCheck = skipTimeValueCheck;
  }

  public Map<String, ChunkCompressionType> getRawIndexCompressionType() {
    return _rawIndexCompressionType;
  }

  public void setRawIndexCompressionType(Map<String, ChunkCompressionType> rawIndexCompressionType) {
    _rawIndexCompressionType.clear();
    _rawIndexCompressionType.putAll(rawIndexCompressionType);
  }

  public String getMetrics() {
    return getQualifyingFields(FieldType.METRIC, true);
  }

  public String getDimensions() {
    return getQualifyingFields(FieldType.DIMENSION, true);
  }

  public String getDateTimeColumnNames() {
    return getQualifyingFields(FieldType.DATE_TIME, true);
  }

  public void setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
    _segmentPartitionConfig = segmentPartitionConfig;
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }

  /**
   * Returns a comma separated list of qualifying field name strings
   * @param type FieldType to filter on
   * @return Comma separate qualifying fields names.
   */
  private String getQualifyingFields(FieldType type, boolean excludeVirtualColumns) {
    List<String> fields = new ArrayList<>();

    for (FieldSpec fieldSpec : getSchema().getAllFieldSpecs()) {
      if (excludeVirtualColumns && fieldSpec.isVirtualColumn()) {
        continue;
      }

      if (fieldSpec.getFieldType() == type) {
        fields.add(fieldSpec.getName());
      }
    }

    Collections.sort(fields);
    return StringUtils.join(fields, ",");
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  public boolean isFailOnEmptySegment() {
    return _failOnEmptySegment;
  }

  public void setFailOnEmptySegment(boolean failOnEmptySegment) {
    _failOnEmptySegment = failOnEmptySegment;
  }
}
