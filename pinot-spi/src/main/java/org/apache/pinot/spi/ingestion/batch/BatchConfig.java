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
package org.apache.pinot.spi.ingestion.batch;

import java.util.Map;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/**
 * Provides all config related to the batch data source, as configured in the table config's ingestion config
 */
public class BatchConfig {
  private final Map<String, String> _batchConfigMap;
  private final String _tableNameWithType;

  private final FileFormat _inputFormat;
  private final String _inputDirURI;
  private final String _inputFsClassName;
  private final Map<String, String> _inputFsProps;

  private final String _outputDirURI;
  private final String _outputFsClassName;
  private final Map<String, String> _outputFsProps;
  private final boolean _overwriteOutput;

  private final String _recordReaderClassName;
  private final String _recordReaderConfigClassName;
  private final Map<String, String> _recordReaderProps;

  private final String _segmentNameGeneratorType;
  private final Map<String, String> _segmentNameGeneratorConfigs;
  private final String _segmentName;
  private final String _segmentNamePrefix;
  private final String _segmentNamePostfix;
  private final boolean _excludeSequenceId;
  private final boolean _appendUUIDToSegmentName;
  private final boolean _omitTimestampsFromSegmentName;
  private final String _sequenceId;

  private final String _pushMode;
  private final int _pushAttempts;
  private final int _pushParallelism;
  private final long _pushIntervalRetryMillis;
  private final String _pushSegmentURIPrefix;
  private final String _pushSegmentURISuffix;
  private final String _pushControllerURI;
  private final String _outputSegmentDirURI;

  public BatchConfig(String tableNameWithType, Map<String, String> batchConfigsMap) {
    _batchConfigMap = batchConfigsMap;
    _tableNameWithType = tableNameWithType;

    String inputFormat = batchConfigsMap.get(BatchConfigProperties.INPUT_FORMAT);
    if (inputFormat != null) {
      _inputFormat = FileFormat.valueOf(inputFormat.toUpperCase());
    } else {
      _inputFormat = null;
    }
    _inputDirURI = batchConfigsMap.get(BatchConfigProperties.INPUT_DIR_URI);
    _inputFsClassName = batchConfigsMap.get(BatchConfigProperties.INPUT_FS_CLASS);
    _inputFsProps =
        IngestionConfigUtils.extractPropsMatchingPrefix(batchConfigsMap, BatchConfigProperties.INPUT_FS_PROP_PREFIX);

    _outputDirURI = batchConfigsMap.get(BatchConfigProperties.OUTPUT_DIR_URI);
    _outputFsClassName = batchConfigsMap.get(BatchConfigProperties.OUTPUT_FS_CLASS);
    _outputFsProps =
        IngestionConfigUtils.extractPropsMatchingPrefix(batchConfigsMap, BatchConfigProperties.OUTPUT_FS_PROP_PREFIX);
    _overwriteOutput = Boolean.parseBoolean(batchConfigsMap.get(BatchConfigProperties.OVERWRITE_OUTPUT));

    _recordReaderClassName = batchConfigsMap.get(BatchConfigProperties.RECORD_READER_CLASS);
    _recordReaderConfigClassName = batchConfigsMap.get(BatchConfigProperties.RECORD_READER_CONFIG_CLASS);
    _recordReaderProps = IngestionConfigUtils.extractPropsMatchingPrefix(batchConfigsMap,
        BatchConfigProperties.RECORD_READER_PROP_PREFIX);

    _segmentNameGeneratorType = IngestionConfigUtils.getSegmentNameGeneratorType(batchConfigsMap);
    _segmentNameGeneratorConfigs = IngestionConfigUtils.extractPropsMatchingPrefix(batchConfigsMap,
        BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX);
    Map<String, String> segmentNameGeneratorProps = IngestionConfigUtils.getSegmentNameGeneratorProps(batchConfigsMap);
    _segmentName = segmentNameGeneratorProps.get(BatchConfigProperties.SEGMENT_NAME);
    _segmentNamePrefix = segmentNameGeneratorProps.get(BatchConfigProperties.SEGMENT_NAME_PREFIX);
    _segmentNamePostfix = segmentNameGeneratorProps.get(BatchConfigProperties.SEGMENT_NAME_POSTFIX);
    _excludeSequenceId = Boolean.parseBoolean(segmentNameGeneratorProps.get(BatchConfigProperties.EXCLUDE_SEQUENCE_ID));
    _sequenceId = batchConfigsMap.get(BatchConfigProperties.SEQUENCE_ID);
    _appendUUIDToSegmentName =
        Boolean.parseBoolean(segmentNameGeneratorProps.get(BatchConfigProperties.APPEND_UUID_TO_SEGMENT_NAME));
    _omitTimestampsFromSegmentName =
        Boolean.parseBoolean(segmentNameGeneratorProps.get(BatchConfigProperties.OMIT_TIMESTAMPS_IN_SEGMENT_NAME));

    _pushMode = IngestionConfigUtils.getPushMode(batchConfigsMap);
    _pushAttempts = IngestionConfigUtils.getPushAttempts(batchConfigsMap);
    _pushParallelism = IngestionConfigUtils.getPushParallelism(batchConfigsMap);
    _pushIntervalRetryMillis = IngestionConfigUtils.getPushRetryIntervalMillis(batchConfigsMap);
    _pushSegmentURIPrefix = batchConfigsMap.get(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX);
    _pushSegmentURISuffix = batchConfigsMap.get(BatchConfigProperties.PUSH_SEGMENT_URI_SUFFIX);
    _pushControllerURI = batchConfigsMap.get(BatchConfigProperties.PUSH_CONTROLLER_URI);
    _outputSegmentDirURI = batchConfigsMap.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI);
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getInputDirURI() {
    return _inputDirURI;
  }

  public String getOutputDirURI() {
    return _outputDirURI;
  }

  public String getInputFsClassName() {
    return _inputFsClassName;
  }

  public Map<String, String> getInputFsProps() {
    return _inputFsProps;
  }

  public String getOutputFsClassName() {
    return _outputFsClassName;
  }

  public Map<String, String> getOutputFsProps() {
    return _outputFsProps;
  }

  public FileFormat getInputFormat() {
    return _inputFormat;
  }

  public boolean isOverwriteOutput() {
    return _overwriteOutput;
  }

  public String getRecordReaderClassName() {
    return _recordReaderClassName;
  }

  public String getRecordReaderConfigClassName() {
    return _recordReaderConfigClassName;
  }

  public Map<String, String> getRecordReaderProps() {
    return _recordReaderProps;
  }

  public Map<String, String> getSegmentNameGeneratorConfigs() {
    return _segmentNameGeneratorConfigs;
  }

  public String getSegmentNameGeneratorType() {
    return _segmentNameGeneratorType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getSegmentNamePrefix() {
    return _segmentNamePrefix;
  }

  public String getSegmentNamePostfix() {
    return _segmentNamePostfix;
  }

  public boolean isExcludeSequenceId() {
    return _excludeSequenceId;
  }

  public String getSequenceId() {
    return _sequenceId;
  }

  public boolean isAppendUUIDToSegmentName() {
    return _appendUUIDToSegmentName;
  }

  public boolean isOmitTimestampsFromSegmentName() {
    return _omitTimestampsFromSegmentName;
  }

  public String getPushMode() {
    return _pushMode;
  }

  public int getPushAttempts() {
    return _pushAttempts;
  }

  public int getPushParallelism() {
    return _pushParallelism;
  }

  public long getPushIntervalRetryMillis() {
    return _pushIntervalRetryMillis;
  }

  public String getPushSegmentURIPrefix() {
    return _pushSegmentURIPrefix;
  }

  public String getPushSegmentURISuffix() {
    return _pushSegmentURISuffix;
  }

  public String getPushControllerURI() {
    return _pushControllerURI;
  }

  public String getOutputSegmentDirURI() {
    return _outputSegmentDirURI;
  }

  public Map<String, String> getBatchConfigMap() {
    return _batchConfigMap;
  }
}
