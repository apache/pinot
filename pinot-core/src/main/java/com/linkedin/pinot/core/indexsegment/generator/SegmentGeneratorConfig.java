/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Joiner;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Configuration properties used in the creation of index segments.
 *
 *
 */
public class SegmentGeneratorConfig {

  private static String SEGMENT_INDEX_VERSION = "segment.index.version";
  private static String SEGMENT_TIME_COLUMN_NAME = "segment.time.column.name";

  private static String SEGMENT_START_TIME = "segment.start.time";
  private static String SEGMENT_END_TIME = "segment.end.time";
  private static String SEGMENT_TIME_UNIT = "segment.time.unit";
  private static String SEGMENT_CREATION_TIME = "segment.creation.time";
  private static final String DATA_INPUT_FORMAT = "data.input.format";
  private static final String DATA_INPUT_FILE_PATH = "data.input.file.path";
  private static final String INDEX_OUTPUT_DIR = "index.output.dir";
  private static String DATA_SCHEMA_PROJECTED_COLUMN = "data.schema.projected.column";
  private static String DATA_SCHEMA = "data.schema";
  private static String DELIMETER = "delimeter";
  private static String FIELD_TYPE = "field.type";
  private static final String IS_SINGLE_VALUED_FIELD = "isSingleValued";
  private static final String FIELD_DATA_TYPE = "dataType";
  private static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";
  private static final String SEGMENT_NAME = "segment.name";

  private static String COMMA = ",";
  private static String DOT = ".";

  private Map<String, Object> properties;
  private Schema schema;
  private RecordReaderConfig recordReaderConfig;

  private boolean createInvertedIndex = false;

  /*
   *
   * Segment metadata, needed properties to sucessfull create the segment
   *
   * */

  public SegmentGeneratorConfig(Schema schema) {
    properties = new HashMap<String, Object>();
    this.schema = schema;
  }

  public void setSegmentNamePostfix(String prefix) {
    properties.put(SEGMENT_NAME_POSTFIX, prefix);
  }

  public String getSegmentNamePostfix() {
    if (properties.containsKey(SEGMENT_NAME_POSTFIX)) {
      return properties.get(SEGMENT_NAME_POSTFIX).toString();
    }
    return null;
  }

  public void setCreateInvertedIndex(boolean create) {
    this.createInvertedIndex = create;
  }

  public boolean createInvertedIndexEnabled() {
    return this.createInvertedIndex;
  }

  public void setSegmentName(String segmentName) {
    properties.put(SEGMENT_NAME, segmentName);
  }

  public boolean containsKey(String key) {
    return properties.containsKey(key);
  }

  public String getSegmentName() {
    if (properties.containsKey(SEGMENT_NAME)) {
      return properties.get(SEGMENT_NAME).toString();
    } else {
      return null;
    }
  }

  public String getString(String key) {
    return properties.get(key).toString();
  }

  public void setTableName(String resourceName) {
    properties.put(MetadataKeys.Segment.TABLE_NAME, resourceName);
  }

  public String getTableName() {
    return properties.get(MetadataKeys.Segment.TABLE_NAME).toString();
  }

  public String getDimensions() {
    return getQualifyingDimensions(FieldType.DIMENSION);
  }

  public String getMetrics() {
    return getQualifyingDimensions(FieldType.METRIC);
  }

  public void setTimeColumnName(String name) {
    properties.put(SEGMENT_TIME_COLUMN_NAME, name);
  }

  public String getTimeColumnName() {
    if (properties.containsKey(SEGMENT_TIME_COLUMN_NAME)) {
      return properties.get(SEGMENT_TIME_COLUMN_NAME).toString();
    }
    return getQualifyingDimensions(FieldType.TIME);
  }

  public void setTimeUnitForSegment(TimeUnit timeUnit) {
    properties.put(MetadataKeys.Segment.TIME_UNIT, timeUnit.toString());
  }

  public TimeUnit getTimeUnitForSegment() {
    if (properties.containsKey(MetadataKeys.Segment.TIME_UNIT)) {
      return TimeUnit.valueOf(properties.get(MetadataKeys.Segment.TIME_UNIT).toString());
    } else {
      if (schema.getTimeFieldSpec() != null) {
        if (schema.getTimeFieldSpec().getOutgoingGranularitySpec() != null) {
          return schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType();
        }
        if (schema.getTimeFieldSpec().getIncomingGranularitySpec() != null) {
          return schema.getTimeFieldSpec().getIncomingGranularitySpec().getTimeType();
        }
      }
      return TimeUnit.DAYS;
    }
  }

  public void setCustom(String key, String value) {
    Joiner j = Joiner.on(",");
    properties.put(j.join(MetadataKeys.Segment.CUSTOM_PROPERTIES_PREFIX, key), value);
  }

  public Map<String, String> getAllCustomKeyValuePair() {
    final Map<String, String> customConfigs = new HashMap<String, String>();
    for (String key : properties.keySet()) {
      if (key.startsWith(MetadataKeys.Segment.CUSTOM_PROPERTIES_PREFIX)) {
        customConfigs.put(key, properties.get(key).toString());
      }
    }
    return customConfigs;
  }

  public void setRecordeReaderConfig(RecordReaderConfig readerConfig) {
    recordReaderConfig = readerConfig;
  }

  public RecordReaderConfig getRecordReaderConfig() {
    return recordReaderConfig;
  }

  private String getQualifyingDimensions(FieldType type) {
    String dims = "";
    for (final FieldSpec spec : getSchema().getAllFieldSpecs()) {
      if (spec.getFieldType() == type) {
        dims += spec.getName() + ",";
      }
    }
    return StringUtils.chomp(dims, ",");
  }

  public void setIndexOutputDir(String dir) {
    properties.put(INDEX_OUTPUT_DIR, dir);
  }

  public String getIndexOutputDir() {
    return properties.get(INDEX_OUTPUT_DIR).toString();
  }

  public void setSegmentVersion(SegmentVersion segmentVersion) {
    properties.put(SEGMENT_INDEX_VERSION, segmentVersion.toString());
  }

  public SegmentVersion getSegmentVersion() {
    return SegmentVersion.valueOf(properties.get(SEGMENT_INDEX_VERSION).toString());
  }

  public void setCreationTime(String creationTime) {
    properties.put(SEGMENT_CREATION_TIME, creationTime);
  }

  public String getCreationTime() {
    return properties.get(SEGMENT_CREATION_TIME).toString();
  }

  public void setStartTime(String startTime) {
    properties.put(SEGMENT_START_TIME, startTime);
  }

  public String getStartTime() {
    return properties.get(SEGMENT_START_TIME).toString();
  }

  public void setEndTime(String endTime) {
    properties.put(SEGMENT_END_TIME, endTime);
  }

  public String getEndTime() {
    return properties.get(SEGMENT_END_TIME).toString();
  }

  public void setTimeUnit(String timeUnit) {
    properties.put(SEGMENT_TIME_UNIT, timeUnit);
  }

  public String getTimeUnit() {
    return properties.get(SEGMENT_TIME_UNIT).toString();
  }

  public FileFormat getInputFileFormat() {
    return FileFormat.valueOf(properties.get(DATA_INPUT_FORMAT).toString());
  }

  public void setInputFileFormat(FileFormat format) {
    properties.put(DATA_INPUT_FORMAT, format.toString());
  }

  public String getInputFilePath() {
    return properties.get(DATA_INPUT_FILE_PATH).toString();
  }

  public void setInputFilePath(String path) {
    properties.put(DATA_INPUT_FILE_PATH, path);
  }

  public List<String> getProjectedColumns() {
    List<String> ret = new ArrayList<String>();
    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      ret.add(spec.getName());
    }
    return ret;
  }

  public Schema getSchema() {
    return schema;
  }

}
