/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.indexsegment.generator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.segment.DefaultSegmentNameGenerator;
import com.linkedin.pinot.core.segment.SegmentNameGenerator;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration properties used in the creation of index segments.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentGeneratorConfig {
  public enum TimeColumnType {
    EPOCH, SIMPLE_DATE
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGeneratorConfig.class);

  private Map<String, String> _customProperties = new HashMap<>();
  private Set<String> _rawIndexCreationColumns = new HashSet<>();
  private Map<String, ChunkCompressorFactory.CompressionType> _rawIndexCompressionType = new HashMap<>();
  private List<String> _invertedIndexCreationColumns = new ArrayList<>();
  private String _dataDir = null;
  private String _inputFilePath = null;
  private FileFormat _format = FileFormat.AVRO;
  private String _outDir = null;
  private boolean _overwrite = false;
  private String _tableName = null;
  private String _segmentName = null;
  private String _segmentNamePostfix = null;
  private String _segmentTimeColumnName = null;
  private TimeUnit _segmentTimeUnit = null;
  private String _segmentCreationTime = null;
  private String _segmentStartTime = null;
  private String _segmentEndTime = null;
  private SegmentVersion _segmentVersion = SegmentVersion.v3;
  private String _schemaFile = null;
  private Schema _schema = null;
  private String _readerConfigFile = null;
  private RecordReaderConfig _readerConfig = null;
  private boolean _enableStarTreeIndex = false;
  private StarTreeIndexSpec _starTreeIndexSpec = null;
  private String _creatorVersion = null;
  private HllConfig _hllConfig = null;
  private SegmentNameGenerator _segmentNameGenerator = null;
  private SegmentPartitionConfig _segmentPartitionConfig = null;
  private int _sequenceId = -1;
  private TimeColumnType _timeColumnType = TimeColumnType.EPOCH;
  private String _simpleDateFormat = null;
  // Use on-heap or off-heap memory to generate index (currently only affect inverted index)
  private boolean _onHeap = false;

  public SegmentGeneratorConfig() {
  }

  /**
   * @deprecated To be replaced by a builder pattern. Use set methods in the meantime.
   * For now, this works only if no setters are called after this copy constructor.
   * @param config to copy from
   */
  @Deprecated
  public SegmentGeneratorConfig(SegmentGeneratorConfig config) {
    Preconditions.checkNotNull(config);
    _customProperties.putAll(config._customProperties);
    _rawIndexCreationColumns.addAll(config._rawIndexCreationColumns);
    _rawIndexCompressionType.putAll(config._rawIndexCompressionType);
    _invertedIndexCreationColumns.addAll(config._invertedIndexCreationColumns);
    _dataDir = config._dataDir;
    _inputFilePath = config._inputFilePath;
    _format = config._format;
    _outDir = config._outDir;
    _overwrite = config._overwrite;
    _tableName = config._tableName;
    _segmentName = config._segmentName;
    _segmentNamePostfix = config._segmentNamePostfix;
    _segmentTimeColumnName = config._segmentTimeColumnName;
    _segmentTimeUnit = config._segmentTimeUnit;
    _segmentCreationTime = config._segmentCreationTime;
    _segmentStartTime = config._segmentStartTime;
    _segmentEndTime = config._segmentEndTime;
    _segmentVersion = config._segmentVersion;
    _schemaFile = config._schemaFile;
    _schema = config._schema;
    _readerConfigFile = config._readerConfigFile;
    _readerConfig = config._readerConfig;
    _enableStarTreeIndex = config._enableStarTreeIndex;
    _starTreeIndexSpec = config._starTreeIndexSpec;
    _creatorVersion = config._creatorVersion;
    _hllConfig = config._hllConfig;
    _segmentNameGenerator = config._segmentNameGenerator;
    _segmentPartitionConfig = config._segmentPartitionConfig;
    _sequenceId = config._sequenceId;
    _timeColumnType = config._timeColumnType;
    _simpleDateFormat = config._simpleDateFormat;
    _onHeap = config._onHeap;
  }

  public SegmentGeneratorConfig(Schema schema) {
    _schema = schema;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public void setCustomProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    _customProperties.putAll(properties);
  }

  public void setSimpleDateFormat(@Nonnull String simpleDateFormat) {
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

  public void setRawIndexCreationColumns(List<String> rawIndexCreationColumns) {
    Preconditions.checkNotNull(rawIndexCreationColumns);
    _rawIndexCreationColumns.addAll(rawIndexCreationColumns);
  }

  public void setInvertedIndexCreationColumns(List<String> indexCreationColumns) {
    Preconditions.checkNotNull(indexCreationColumns);
    _invertedIndexCreationColumns.addAll(indexCreationColumns);
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

  public String getDataDir() {
    return _dataDir;
  }

  public void setDataDir(String dataDir) {
    _dataDir = dataDir;
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

  public String getOutDir() {
    return _outDir;
  }

  public void setOutDir(String dir) {
    Preconditions.checkNotNull(dir);
    final File outputDir = new File(dir);
    if (outputDir.exists()) {
      Preconditions.checkState(outputDir.isDirectory(), "Path {} is not a directory.", dir);
    } else {
      Preconditions.checkState(outputDir.mkdirs(), "Cannot create output dir: {}", dir);
    }
    _outDir = outputDir.getAbsolutePath();
  }

  public boolean isOverwrite() {
    return _overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    _overwrite = overwrite;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
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
    if (_segmentTimeColumnName != null) {
      return _segmentTimeColumnName;
    }
    return getQualifyingFields(FieldType.TIME);
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
    if (_segmentTimeUnit != null) {
      return _segmentTimeUnit;
    } else {
      TimeFieldSpec timeFieldSpec = _schema.getTimeFieldSpec();
      if (timeFieldSpec != null) {
        // Outgoing granularity is always non-null.
        return timeFieldSpec.getOutgoingGranularitySpec().getTimeType();
      }
      return TimeUnit.DAYS;
    }
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

  public String getSchemaFile() {
    return _schemaFile;
  }

  public void setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
  }

  public Schema getSchema() {
    return _schema;
  }

  public void setSchema(Schema schema) {
    Preconditions.checkNotNull(schema);
    _schema = schema;
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

  public String getReaderConfigFile() {
    return _readerConfigFile;
  }

  public void setReaderConfigFile(String readerConfigFile) {
    _readerConfigFile = readerConfigFile;
  }

  public RecordReaderConfig getReaderConfig() {
    return _readerConfig;
  }

  public void setReaderConfig(RecordReaderConfig readerConfig) {
    _readerConfig = readerConfig;
  }

  public boolean isEnableStarTreeIndex() {
    return _enableStarTreeIndex;
  }

  public void setEnableStarTreeIndex(boolean enableStarTreeIndex) {
    _enableStarTreeIndex = enableStarTreeIndex;
  }

  public StarTreeIndexSpec getStarTreeIndexSpec() {
    return _starTreeIndexSpec;
  }

  public void setStarTreeIndexSpec(StarTreeIndexSpec starTreeIndexSpec) {
    _starTreeIndexSpec = starTreeIndexSpec;
  }

  /**
   * Enable star tree generation with the given indexing spec.
   * <p>NOTE: DO NOT remove the setter and getter for these two fields, they are required for ser/de.
   *
   * @param starTreeIndexSpec Optional indexing spec for star tree
   */
  public void enableStarTreeIndex(@Nullable StarTreeIndexSpec starTreeIndexSpec) {
    setEnableStarTreeIndex(true);
    setStarTreeIndexSpec(starTreeIndexSpec);
  }

  public HllConfig getHllConfig() {
    return _hllConfig;
  }

  public void setHllConfig(HllConfig hllConfig) {
    _hllConfig = hllConfig;
  }

  public SegmentNameGenerator getSegmentNameGenerator() {
    if (_segmentNameGenerator != null) {
      return _segmentNameGenerator;
    }
    if (_segmentName != null) {
      return new DefaultSegmentNameGenerator(_segmentName);
    }
    return new DefaultSegmentNameGenerator(getTimeColumnName(), getTableName(), getSegmentNamePostfix(),
        getSequenceId());
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

  @JsonIgnore
  public ChunkCompressorFactory.CompressionType getColumnCompressionType(String columnName) {
    if (_rawIndexCompressionType.containsKey(columnName)) {
      return _rawIndexCompressionType.get(columnName);
    }
    return ChunkCompressorFactory.CompressionType.SNAPPY;
  }

  public Map<String, ChunkCompressorFactory.CompressionType> getRawIndexCompressionType() {
    return _rawIndexCompressionType;
  }

  public void setRawIndexCompressionType(Map<String, ChunkCompressorFactory.CompressionType> rawIndexCompressionType) {
    _rawIndexCompressionType.clear();
    _rawIndexCompressionType.putAll(rawIndexCompressionType);
  }

  @JsonIgnore
  public String getMetrics() {
    return getQualifyingFields(FieldType.METRIC);
  }

  /**
   * @deprecated Load outside the class and use the setter for schema setting.
   * @throws IOException
   */
  @Deprecated
  public void loadConfigFiles() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();

    Schema schema;
    if (_schemaFile != null) {
      schema = Schema.fromFile(new File(_schemaFile));
      setSchema(schema);
    } else if (_format == FileFormat.AVRO) {
      schema = AvroUtils.extractSchemaFromAvro(new File(_inputFilePath));
      setSchema(schema);
    } else {
      throw new RuntimeException("Input format " + _format + " requires schema.");
    }
    setTimeColumnName(schema.getTimeColumnName());
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      setSegmentTimeUnit(timeFieldSpec.getIncomingGranularitySpec().getTimeType());
    } else {
      setSegmentTimeUnit(TimeUnit.DAYS);
    }

    if (_readerConfigFile != null) {
      setReaderConfig(objectMapper.readValue(new File(_readerConfigFile), CSVRecordReaderConfig.class));
    }
  }

  @JsonIgnore
  public String getDimensions() {
    return getQualifyingFields(FieldType.DIMENSION);
  }

  @JsonIgnore
  public String getDateTimeColumnNames() {
    return getQualifyingFields(FieldType.DATE_TIME);
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
  @JsonIgnore
  private String getQualifyingFields(FieldType type) {
    List<String> fields = new ArrayList<>();

    for (final FieldSpec spec : getSchema().getAllFieldSpecs()) {
      if (spec.getFieldType() == type) {
        fields.add(spec.getName());
      }
    }
    Collections.sort(fields);
    return StringUtils.join(fields, ",");
  }
}
