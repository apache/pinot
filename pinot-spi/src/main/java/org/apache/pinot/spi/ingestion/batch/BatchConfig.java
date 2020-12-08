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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.FileFormat;


/**
 * Provides all config related to the batch data source, as configured in the table config's ingestion config
 */
public class BatchConfig {
  private final Map<String, String> _batchConfigMap;

  private final String _tableNameWithType;
  private final String _inputDirURI;
  private final String _outputDirURI;
  private final String _inputFsClassName;
  private final Map<String, String> _inputFsProps = new HashMap<>();
  private final String _outputFsClassName;
  private final Map<String, String> _outputFsProps = new HashMap<>();
  private final FileFormat _inputFormat;
  private final String _recordReaderClassName;
  private final String _recordReaderConfigClassName;
  private final Map<String, String> _recordReaderProps = new HashMap<>();

  public BatchConfig(String tableNameWithType, Map<String, String> batchConfigsMap) {
    _batchConfigMap = batchConfigsMap;
    _tableNameWithType = tableNameWithType;
    String inputFormat = batchConfigsMap.get(BatchConfigProperties.INPUT_FORMAT);
    Preconditions.checkState(inputFormat != null, "Property: %s cannot be null", BatchConfigProperties.INPUT_FORMAT);
    _inputFormat = FileFormat.valueOf(inputFormat.toUpperCase());
    _inputDirURI = batchConfigsMap.get(BatchConfigProperties.INPUT_DIR_URI);
    _inputFsClassName = batchConfigsMap.get(BatchConfigProperties.INPUT_FS_CLASS);
    _outputDirURI = batchConfigsMap.get(BatchConfigProperties.OUTPUT_DIR_URI);
    _outputFsClassName = batchConfigsMap.get(BatchConfigProperties.OUTPUT_FS_CLASS);
    _recordReaderClassName = batchConfigsMap.get(BatchConfigProperties.RECORD_READER_CLASS);
    _recordReaderConfigClassName = batchConfigsMap.get(BatchConfigProperties.RECORD_READER_CONFIG_CLASS);
    for (Map.Entry<String, String> entry : batchConfigsMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(BatchConfigProperties.INPUT_FS_PROP_PREFIX)) {
        _inputFsProps.put(key, entry.getValue());
      }
      if (key.startsWith(BatchConfigProperties.OUTPUT_FS_PROP_PREFIX)) {
        _outputFsProps.put(key, entry.getValue());
      }
      if (key.startsWith(BatchConfigProperties.RECORD_READER_PROP_PREFIX)) {
        _recordReaderProps.put(key, entry.getValue());
      }
    }
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

  public String getRecordReaderClassName() {
    return _recordReaderClassName;
  }

  public String getRecordReaderConfigClassName() {
    return _recordReaderConfigClassName;
  }

  public Map<String, String> getRecordReaderProps() {
    return _recordReaderProps;
  }

  public Map<String, String> getBatchConfigMap() {
    return _batchConfigMap;
  }
}
