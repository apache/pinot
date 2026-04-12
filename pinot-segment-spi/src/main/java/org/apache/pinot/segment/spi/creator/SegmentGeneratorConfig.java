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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.name.FixedSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.segment.spi.creator.name.UploadedRealtimeSegmentNameGenerator;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Configuration properties used in the creation of index segments.
 */
public class SegmentGeneratorConfig implements Serializable {
  public enum TimeColumnType {
    EPOCH, SIMPLE_DATE
  }

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final Map<String, FieldIndexConfigs> _indexConfigsByColName;
  private final Map<String, Map<String, String>> _columnProperties = new HashMap<>();
  // NOTE: Use TreeMap to guarantee the order. The custom properties will be written into the segment metadata.
  private final TreeMap<String, String> _customProperties = new TreeMap<>();
  private final List<String> _columnSortOrder = new ArrayList<>();
  private String _inputFilePath = null;
  private FileFormat _format = FileFormat.AVRO;
  private String _recordReaderPath = null; //TODO: this should be renamed to recordReaderClass or even better removed
  private String _outDir = null;
  private String _rawTableName = null;
  private String _segmentName = null;
  private String _segmentNamePrefix = null;
  private String _segmentNamePostfix = null;
  private String _segmentTimeColumnName = null;
  private FieldSpec.DataType _segmentTimeColumnDataType = null;
  private TimeUnit _segmentTimeUnit = null;
  private String _segmentCreationTime = null;
  private String _segmentStartTime = null;
  private String _segmentEndTime = null;
  private SegmentVersion _segmentVersion = SegmentVersion.v3;
  private RecordReaderConfig _readerConfig = null;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs = null;
  private MultiColumnTextIndexConfig _multiColumnTextIndexConfig;
  private boolean _enableDefaultStarTree = false;
  private String _creatorVersion = null;
  private SegmentNameGenerator _segmentNameGenerator = null;
  private SegmentPartitionConfig _segmentPartitionConfig = null;

  private int _uploadedSegmentPartitionId = -1;
  private int _sequenceId = -1;
  private TimeColumnType _timeColumnType = TimeColumnType.EPOCH;
  private DateTimeFormatSpec _dateTimeFormatSpec = null;
  // Use on-heap or off-heap memory to generate index (currently only affect inverted index and star-tree v2)
  private boolean _onHeap = false;
  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   */
  private boolean _defaultNullHandlingEnabled = false;
  private boolean _continueOnError = false;
  private boolean _rowTimeValueCheck = false;
  private boolean _segmentTimeValueCheck = true;
  private boolean _failOnEmptySegment = false;
  private boolean _optimizeDictionary = false;
  private boolean _optimizeDictionaryForMetrics = false;
  private boolean _optimizeDictionaryType = false;
  private double _noDictionarySizeRatioThreshold = IndexingConfig.DEFAULT_NO_DICTIONARY_SIZE_RATIO_THRESHOLD;
  private Double _noDictionaryCardinalityRatioThreshold;
  private boolean _realtimeConversion = false;
  // consumerDir contains data from the consuming segment, and is used during _realtimeConversion optimization
  private File _consumerDir;
  private SegmentZKPropsConfig _segmentZKPropsConfig;
  // Whether the mutable segment is compacted
  private boolean _mutableSegmentCompacted = false;
  // Available when converting a mutable segment to immutable segment with both sorting and reusing mutable text index
  // enabled. Index is mutable docId, value is immutable docId.
  private int[] _mutableToImmutableDocIdMap;

  // Type of the instance (SERVER/MINION) that is trying to create the segment.
  private InstanceType _instanceType;

  /**
   * Constructs the SegmentGeneratorConfig with table config and schema.
   * NOTE: The passed in table config and schema might be changed.
   */
  public SegmentGeneratorConfig(TableConfig tableConfig, Schema schema) {
    Preconditions.checkNotNull(tableConfig);
    Preconditions.checkNotNull(schema);
    TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);
    _tableConfig = tableConfig;
    _schema = schema;
    _indexConfigsByColName = FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema);
    setTableName(tableConfig.getTableName());

    // NOTE: SegmentGeneratorConfig#setSchema doesn't set the time column anymore. timeColumnName is expected to be
    // read from table config.
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (timeColumnName != null) {
      setTime(timeColumnName, schema);
    }

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    String segmentVersion = indexingConfig.getSegmentFormatVersion();
    if (segmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(segmentVersion);
    }

    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      _columnSortOrder.addAll(sortedColumns);
    }

    _segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    _defaultNullHandlingEnabled = indexingConfig.isNullHandlingEnabled();
    _optimizeDictionary = indexingConfig.isOptimizeDictionary();
    _optimizeDictionaryForMetrics = indexingConfig.isOptimizeDictionaryForMetrics();
    _optimizeDictionaryType = indexingConfig.isOptimizeDictionaryType();
    _noDictionarySizeRatioThreshold = indexingConfig.getNoDictionarySizeRatioThreshold();
    _noDictionaryCardinalityRatioThreshold = indexingConfig.getNoDictionaryCardinalityRatioThreshold();

    // Star-tree configs
    setStarTreeIndexConfigs(indexingConfig.getStarTreeIndexConfigs());
    setEnableDefaultStarTree(indexingConfig.isEnableDefaultStarTree());
    _multiColumnTextIndexConfig = indexingConfig.getMultiColumnTextIndexConfig();

    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    if (fieldConfigs != null) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        Map<String, String> properties = fieldConfig.getProperties();
        if (properties != null) {
          _columnProperties.put(fieldConfig.getName(), Collections.unmodifiableMap(properties));
        }
      }
    }

    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null) {
      _continueOnError = ingestionConfig.isContinueOnError();
      _rowTimeValueCheck = ingestionConfig.isRowTimeValueCheck();
      _segmentTimeValueCheck = ingestionConfig.isSegmentTimeValueCheck();
    }
  }

  /**
   * Returns the {@link TableConfig} that was used to initialize this object.
   *
   * Remember that this object is mutable. Therefore it may have modified since the object was created. Changes on this
   * object may or may not modify the initial table config, so the object returned by this method may not contain the
   * same information stored on this SegmentGeneratorConfig. For example, if someone called
   * {@link #setTimeColumnName(String)} on the SegmentGeneratorConfig, the TableConfig returned by this method
   * will not be modified accordingly.
   */
  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public Map<String, FieldIndexConfigs> getIndexConfigsByColName() {
    return _indexConfigsByColName;
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    return Collections.unmodifiableMap(_columnProperties);
  }

  /**
   * Set time column details using the given time column
   */
  private void setTime(String timeColumnName, Schema schema) {
    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
    if (dateTimeFieldSpec != null) {
      _segmentTimeColumnDataType = dateTimeFieldSpec.getDataType();
      setTimeColumnName(dateTimeFieldSpec.getName());
      setDateTimeFormatSpec(dateTimeFieldSpec.getFormatSpec());
    }
  }

  public Map<String, String> getCustomProperties() {
    return Collections.unmodifiableMap(_customProperties);
  }

  public void setCustomProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    _customProperties.putAll(properties);
  }

  public void setDateTimeFormatSpec(DateTimeFormatSpec formatSpec) {
    _dateTimeFormatSpec = formatSpec;
    if (formatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT) {
      // timeUnit is only needed by EPOCH time format.
      _timeColumnType = TimeColumnType.SIMPLE_DATE;
    } else {
      _segmentTimeUnit = formatSpec.getColumnUnit();
      _timeColumnType = TimeColumnType.EPOCH;
    }
  }

  @Nullable
  public DateTimeFormatSpec getDateTimeFormatSpec() {
    return _dateTimeFormatSpec;
  }

  public TimeColumnType getTimeColumnType() {
    return _timeColumnType;
  }

  public List<String> getColumnSortOrder() {
    return Collections.unmodifiableList(_columnSortOrder);
  }

  public String getInputFilePath() {
    return _inputFilePath;
  }

  public void setInputFilePath(String inputFilePath) {
    Preconditions.checkNotNull(inputFilePath);
    File inputFile = new File(inputFilePath);
    Preconditions.checkState(inputFile.exists(), "Input path %s does not exist.", inputFilePath);
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

  public String getSegmentNamePrefix() {
    return _segmentNamePrefix;
  }

  public void setSegmentNamePrefix(String segmentNamePrefix) {
    _segmentNamePrefix = segmentNamePrefix;
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

  public int getUploadedSegmentPartitionId() {
    return _uploadedSegmentPartitionId;
  }

  public int getSequenceId() {
    return _sequenceId;
  }

  /**
   * Use this method to add partitionId if it is generated externally during segment upload
   */
  public void setUploadedSegmentPartitionId(int partitionId) {
    _uploadedSegmentPartitionId = partitionId;
  }

  /**
   * This method should be used instead of setPostfix if you are adding a sequence number.
   */
  public void setSequenceId(int sequenceId) {
    _sequenceId = sequenceId;
  }

  @Nullable
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

  public RecordReaderConfig getReaderConfig() {
    return _readerConfig;
  }

  public void setReaderConfig(RecordReaderConfig readerConfig) {
    _readerConfig = readerConfig;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    if (_starTreeIndexConfigs == null) {
      return null;
    }
    return Collections.unmodifiableList(_starTreeIndexConfigs);
  }

  public void setStarTreeIndexConfigs(List<StarTreeIndexConfig> starTreeIndexConfigs) {
    _starTreeIndexConfigs = starTreeIndexConfigs;
  }

  @Nullable
  public MultiColumnTextIndexConfig getMultiColumnTextIndexConfig() {
    return _multiColumnTextIndexConfig;
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

    String segmentNameGeneratorType = inferSegmentNameGeneratorType();
    switch (segmentNameGeneratorType) {
      case BatchConfigProperties.SegmentNameGeneratorType.FIXED:
        return new FixedSegmentNameGenerator(_segmentName);
      case BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE:
        return new NormalizedDateSegmentNameGenerator(_rawTableName, _segmentNamePrefix, false,
            IngestionConfigUtils.getBatchSegmentIngestionType(_tableConfig),
            IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig), _dateTimeFormatSpec,
            _segmentNamePostfix);
      case BatchConfigProperties.SegmentNameGeneratorType.UPLOADED_REALTIME:
        return new UploadedRealtimeSegmentNameGenerator(_rawTableName, _uploadedSegmentPartitionId,
            Long.parseLong(_segmentCreationTime), _segmentNamePrefix, _segmentNamePostfix);
      default:
        return new SimpleSegmentNameGenerator(_segmentNamePrefix != null ? _segmentNamePrefix : _rawTableName,
            _segmentNamePostfix);
    }
  }

  /**
   * Infers the segment name generator type based on segment generator config properties. Will default to simple
   * SegmentNameGeneratorType.
   */
  public String inferSegmentNameGeneratorType() {
    if (_segmentName != null) {
      return BatchConfigProperties.SegmentNameGeneratorType.FIXED;
    }

    if (_segmentTimeColumnDataType == FieldSpec.DataType.STRING && _timeColumnType == TimeColumnType.SIMPLE_DATE) {
      return BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE;
    }

    // if segment is externally partitioned
    if (_uploadedSegmentPartitionId != -1) {
      return BatchConfigProperties.SegmentNameGeneratorType.UPLOADED_REALTIME;
    }

    return BatchConfigProperties.SegmentNameGeneratorType.SIMPLE;
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
    return !_segmentTimeValueCheck;
  }

  public void setSkipTimeValueCheck(boolean skipTimeValueCheck) {
    _segmentTimeValueCheck = !skipTimeValueCheck;
  }

  public List<String> getMetrics() {
    return getQualifyingFields(FieldType.METRIC, true);
  }

  public List<String> getDimensions() {
    return getQualifyingFields(FieldType.DIMENSION, true);
  }

  public List<String> getDateTimeColumnNames() {
    return getQualifyingFields(FieldType.DATE_TIME, true);
  }

  public List<String> getComplexColumnNames() {
    return getQualifyingFields(FieldType.COMPLEX, true);
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
   * @return list of qualifying fields names.
   */
  private List<String> getQualifyingFields(FieldType type, boolean excludeVirtualColumns) {
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
    return fields;
  }

  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   *
   * @deprecated Use {@link #isDefaultNullHandlingEnabled()} instead
   */
  @Deprecated
  public boolean isNullHandlingEnabled() {
    return _defaultNullHandlingEnabled;
  }

  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   */
  public boolean isDefaultNullHandlingEnabled() {
    return _defaultNullHandlingEnabled;
  }

  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   *
   * @deprecated Use {@link #setDefaultNullHandlingEnabled(boolean)} instead
   */
  @Deprecated
  public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
    setDefaultNullHandlingEnabled(nullHandlingEnabled);
  }

  /**
   * Whether null handling is enabled by default. This value is only used if
   * {@link Schema#isEnableColumnBasedNullHandling()} is false.
   */
  public void setDefaultNullHandlingEnabled(boolean nullHandlingEnabled) {
    _defaultNullHandlingEnabled = nullHandlingEnabled;
  }

  public boolean isContinueOnError() {
    return _continueOnError;
  }

  public void setContinueOnError(boolean continueOnError) {
    _continueOnError = continueOnError;
  }

  public boolean isRowTimeValueCheck() {
    return _rowTimeValueCheck;
  }

  public void setRowTimeValueCheck(boolean rowTimeValueCheck) {
    _rowTimeValueCheck = rowTimeValueCheck;
  }

  public boolean isSegmentTimeValueCheck() {
    return _segmentTimeValueCheck;
  }

  public void setSegmentTimeValueCheck(boolean segmentTimeValueCheck) {
    _segmentTimeValueCheck = segmentTimeValueCheck;
  }

  public boolean isOptimizeDictionary() {
    return _optimizeDictionary;
  }

  public void setOptimizeDictionary(boolean optimizeDictionary) {
    _optimizeDictionary = optimizeDictionary;
  }

  public boolean isOptimizeDictionaryForMetrics() {
    return _optimizeDictionaryForMetrics;
  }

  public void setOptimizeDictionaryForMetrics(boolean optimizeDictionaryForMetrics) {
    _optimizeDictionaryForMetrics = optimizeDictionaryForMetrics;
  }

  public boolean isOptimizeDictionaryType() {
    return _optimizeDictionaryType;
  }

  public void setOptimizeDictionaryType(boolean optimizeDictionaryType) {
    _optimizeDictionaryType = optimizeDictionaryType;
  }

  public double getNoDictionarySizeRatioThreshold() {
    return _noDictionarySizeRatioThreshold;
  }

  public boolean isRealtimeConversion() {
    return _realtimeConversion;
  }

  public void setRealtimeConversion(boolean realtimeConversion) {
    _realtimeConversion = realtimeConversion;
  }

  public File getConsumerDir() {
    return _consumerDir;
  }

  public void setConsumerDir(File consumerDir) {
    _consumerDir = consumerDir;
  }

  public void setNoDictionarySizeRatioThreshold(double noDictionarySizeRatioThreshold) {
    _noDictionarySizeRatioThreshold = noDictionarySizeRatioThreshold;
  }

  @Nullable
  public Double getNoDictionaryCardinalityRatioThreshold() {
    return _noDictionaryCardinalityRatioThreshold;
  }

  public void setNoDictionaryCardinalityRatioThreshold(@Nullable Double noDictionaryCardinalityRatioThreshold) {
    _noDictionaryCardinalityRatioThreshold = noDictionaryCardinalityRatioThreshold;
  }

  public boolean isFailOnEmptySegment() {
    return _failOnEmptySegment;
  }

  public void setFailOnEmptySegment(boolean failOnEmptySegment) {
    _failOnEmptySegment = failOnEmptySegment;
  }

  @Nullable
  public SegmentZKPropsConfig getSegmentZKPropsConfig() {
    return _segmentZKPropsConfig;
  }

  public void setSegmentZKPropsConfig(SegmentZKPropsConfig segmentZKPropsConfig) {
    _segmentZKPropsConfig = segmentZKPropsConfig;
  }

  public boolean isMutableSegmentCompacted() {
    return _mutableSegmentCompacted;
  }

  public void setMutableSegmentCompacted(boolean mutableSegmentCompacted) {
    _mutableSegmentCompacted = mutableSegmentCompacted;
  }

  @Nullable
  public int[] getMutableToImmutableDocIdMap() {
    return _mutableToImmutableDocIdMap;
  }

  public void setMutableToImmutableDocIdMap(int[] mutableToImmutableDocIdMap) {
    _mutableToImmutableDocIdMap = mutableToImmutableDocIdMap;
  }

  @Nullable
  public InstanceType getInstanceType() {
    return _instanceType;
  }

  public void setInstanceType(InstanceType instanceType) {
    _instanceType = instanceType;
  }
}
