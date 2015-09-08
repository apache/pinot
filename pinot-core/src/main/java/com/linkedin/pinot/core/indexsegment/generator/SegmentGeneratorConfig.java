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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Configuration properties used in the creation of index segments.
 */
public class SegmentGeneratorConfig {

  private Map<String, String> properties;
  private Schema schema;
  private RecordReaderConfig recordReaderConfig;

  /**
   * For inverted Index : default
   */
  private boolean createInvertedIndex = false;
  private List<String> invertedIndexCreationColumns = new ArrayList<String>();

  private String segmentNamePostfix = null;
  private String segmentName = null;
  private String tableName = null;
  private String segmentTimeColumnName = null;
  private TimeUnit segmentTimeUnit = null;
  private String indexOutputDir = null;
  private SegmentVersion segmentVersion = SegmentVersion.v1;
  private String segmentCreationTime = null;
  private String segmentStartTime = null;
  private String segmentEndTime = null;
  private FileFormat inputFileFormat = FileFormat.AVRO;
  private File inputDataFilePath = null;

  /*
   *
   * Segment metadata, needed properties to sucessfull create the segment
   *
   * */

  public SegmentGeneratorConfig(Schema schema) {
    properties = new HashMap<String, String>();
    this.schema = schema;
  }

  public void setSegmentNamePostfix(String prefix) {
    segmentNamePostfix = prefix;
  }

  public String getSegmentNamePostfix() {
    return segmentNamePostfix;
  }

  /**
   *
   * inverted index creation by default is set to false,
   * it can be turned on, on a per column basis
   */

  @Deprecated
  private void setCreateInvertedIndex(boolean create) {
    createInvertedIndex = create;
  }

  public boolean isCreateInvertedIndexEnabled() {
    return createInvertedIndex;
  }

  public void createInvertedIndexForColumn(String column) {
    if (createInvertedIndex == false) {
      createInvertedIndex = true;
    }
    invertedIndexCreationColumns.add(column);
  }

  public void createInvertedIndexForAllColumns() {
    if (schema == null) {
      throw new RuntimeException(
          "schema cannot be null, make sure that schema is property set before calling this method");
    }
    createInvertedIndex = true;
    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      invertedIndexCreationColumns.add(spec.getName());
    }
  }

  public List<String> getInvertedIndexCreationColumns() {
    return invertedIndexCreationColumns;
  }

  public void setSegmentName(String segmentName) {
    this.segmentName = segmentName;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public void setCustomProperty(String key, String value) {
    properties.put(key, value);
  }

  public boolean containsCustomPropertyWithKey(String key) {
    return properties.containsKey(key);
  }

  public String getCustomProperty(String key) {
    return properties.get(key).toString();
  }

  public void setTableName(String resourceName) {
    tableName = resourceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getDimensions() {
    return getQualifyingDimensions(FieldType.DIMENSION);
  }

  public String getMetrics() {
    return getQualifyingDimensions(FieldType.METRIC);
  }

  public void setTimeColumnName(String name) {
    segmentTimeColumnName = name;
  }

  public String getTimeColumnName() {
    if (segmentTimeColumnName != null) {
      return segmentTimeColumnName;
    }
    return getQualifyingDimensions(FieldType.TIME);
  }

  public void setTimeUnitForSegment(TimeUnit timeUnit) {
    segmentTimeUnit = timeUnit;
  }

  public TimeUnit getTimeUnitForSegment() {
    if (segmentTimeUnit != null) {
      return segmentTimeUnit;
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

  public Map<String, String> getAllCustomKeyValuePair() {
    return properties;
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
    if (new File(dir).exists()) {

    }
    indexOutputDir = dir;
  }

  public String getIndexOutputDir() {
    return indexOutputDir;
  }

  public void setSegmentVersion(SegmentVersion segmentVersion) {
    this.segmentVersion = segmentVersion;
  }

  public SegmentVersion getSegmentVersion() {
    return segmentVersion;
  }

  public void setCreationTime(String creationTime) {
    segmentCreationTime = creationTime;
  }

  public String getCreationTime() {
    return segmentCreationTime;
  }

  public void setStartTime(String startTime) {
    segmentStartTime = startTime;
  }

  public String getStartTime() {
    return segmentStartTime;
  }

  public void setEndTime(String endTime) {
    segmentEndTime = endTime;
  }

  public String getEndTime() {
    return segmentEndTime;
  }

  public void setTimeUnit(String timeUnit) {
    segmentTimeUnit = TimeUnit.valueOf(timeUnit);
  }

  public String getTimeUnit() {
    return segmentTimeUnit.toString();
  }

  public FileFormat getInputFileFormat() {
    return inputFileFormat;
  }

  public void setInputFileFormat(FileFormat format) {
    inputFileFormat = format;
  }

  public String getInputFilePath() {
    return inputDataFilePath.getAbsolutePath();
  }

  public void setInputFilePath(String path) {
    inputDataFilePath = new File(path);
    if (!inputDataFilePath.exists()) {
      throw new RuntimeException("input path needs to exist");
    }
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
