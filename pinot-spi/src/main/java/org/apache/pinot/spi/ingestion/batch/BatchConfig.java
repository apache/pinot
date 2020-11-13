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
  private final String _tableNameWithType;
  private final String _type;
  private final String _inputDirURI;
  private final String _outputDirURI;
  private final String _fsClassName;
  private final Map<String, String> _fsProps = new HashMap<>();
  private final FileFormat _inputFormat;
  private final String _recordReaderClassName;
  private final String _recordReaderConfigClassName;
  private final Map<String, String> _recordReaderProps = new HashMap<>();

  private final Map<String, String> _batchConfigMap = new HashMap<>();

  public BatchConfig(String tableNameWithType, Map<String, String> batchConfigsMap) {
    _tableNameWithType = tableNameWithType;

    _type = batchConfigsMap.get(BatchConfigProperties.BATCH_TYPE);
    Preconditions.checkState(_type != null, "Property: %s cannot be null", BatchConfigProperties.BATCH_TYPE);

    String inputDirURIKey = BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.INPUT_DIR_URI);
    _inputDirURI = batchConfigsMap.get(inputDirURIKey);
    Preconditions.checkState(_inputDirURI != null, "Property: %s cannot be null", inputDirURIKey);

    String outputDirURIKey = BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.OUTPUT_DIR_URI);
    _outputDirURI = batchConfigsMap.get(outputDirURIKey);
    Preconditions.checkState(_outputDirURI != null, "Property: %s cannot be null", outputDirURIKey);

    String fsClassNameKey = BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.FS_CLASS);
    _fsClassName = batchConfigsMap.get(fsClassNameKey);
    Preconditions.checkState(_fsClassName != null, "Property: %s cannot be null", fsClassNameKey);

    String inputFormatKey = BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.INPUT_FORMAT);
    String inputFormat = batchConfigsMap.get(inputFormatKey);
    Preconditions.checkState(inputFormat != null, "Property: %s cannot be null", inputFormat);
    _inputFormat = FileFormat.valueOf(inputFormat.toUpperCase());

    String recordReaderClassNameKey =
        BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.RECORD_READER_CLASS);
    _recordReaderClassName = batchConfigsMap.get(recordReaderClassNameKey);
    Preconditions.checkState(_recordReaderClassName != null, "Property: %s cannot be null", recordReaderClassNameKey);

    String recordReaderConfigClassNameKey =
        BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.RECORD_READER_CONFIG_CLASS);
    _recordReaderConfigClassName = batchConfigsMap.get(recordReaderConfigClassNameKey);

    String fsPropPrefix = BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.FS_PROP_PREFIX);
    String recordReaderPropPrefix =
        BatchConfigProperties.constructBatchProperty(_type, BatchConfigProperties.RECORD_READER_PROP_PREFIX);
    for (Map.Entry<String, String> entry : batchConfigsMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(fsPropPrefix)) {
        _fsProps.put(key, entry.getValue());
      } else if (key.startsWith(recordReaderPropPrefix)) {
        _recordReaderProps.put(key, entry.getValue());
      }
      _batchConfigMap.put(key, entry.getValue());
    }
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getType() {
    return _type;
  }

  public String getInputDirURI() {
    return _inputDirURI;
  }

  public String getOutputDirURI() {
    return _outputDirURI;
  }

  public String getFsClassName() {
    return _fsClassName;
  }

  public Map<String, String> getFsProps() {
    return _fsProps;
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
