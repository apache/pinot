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
package org.apache.pinot.spi.ingestion.batch.spec;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.Schema;


/**
 * SegmentGenerationTaskSpec defines all the required information in order to generate Pinot Segment.
 * Note that this task creates a segment directory, not tar file.
 */
public class SegmentGenerationTaskSpec implements Serializable {
  public static final String CUSTOM_SUBSET = "custom";
  public static final String CUSTOM_PREFIX = CUSTOM_SUBSET + '.';

  /**
   * Table config to create segment
   */
  private JsonNode _tableConfig;

  /**
   * Table schema
   */
  private Schema _schema;

  /**
   * Used to init record reader to read from data file
   */
  private RecordReaderSpec _recordReaderSpec;

  /**
   * Used to generate segment name
   */
  private SegmentNameGeneratorSpec _segmentNameGeneratorSpec;

  /**
   * Data file path
   */
  private String _inputFilePath;

  /**
   * Output segment directory to host all data files
   */
  private String _outputDirectoryPath;

  /**
   * sequence id
   */
  private int _sequenceId;

  /**
   * Custom properties set into segment metadata
   */
  private Map<String, String> _customProperties = new HashMap<>();

  public JsonNode getTableConfig() {
    return _tableConfig;
  }

  public void setTableConfig(JsonNode tableConfig) {
    _tableConfig = tableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public void setSchema(Schema schema) {
    _schema = schema;
  }

  public RecordReaderSpec getRecordReaderSpec() {
    return _recordReaderSpec;
  }

  public void setRecordReaderSpec(RecordReaderSpec recordReaderSpec) {
    _recordReaderSpec = recordReaderSpec;
  }

  public SegmentNameGeneratorSpec getSegmentNameGeneratorSpec() {
    return _segmentNameGeneratorSpec;
  }

  public void setSegmentNameGeneratorSpec(SegmentNameGeneratorSpec segmentNameGeneratorSpec) {
    _segmentNameGeneratorSpec = segmentNameGeneratorSpec;
  }

  public String getInputFilePath() {
    return _inputFilePath;
  }

  public void setInputFilePath(String inputFilePath) {
    _inputFilePath = inputFilePath;
  }

  public String getOutputDirectoryPath() {
    return _outputDirectoryPath;
  }

  public void setOutputDirectoryPath(String outputDirectoryPath) {
    _outputDirectoryPath = outputDirectoryPath;
  }

  public int getSequenceId() {
    return _sequenceId;
  }

  public void setSequenceId(int sequenceId) {
    _sequenceId = sequenceId;
  }

  public void setCustomProperty(String key, String value) {
    if (!key.startsWith(CUSTOM_PREFIX)) {
      key = CUSTOM_PREFIX + key;
    }
    _customProperties.put(key, value);
  }

  public void setCustomProperties(Map<String, String> customProperties) {
    for (String key : customProperties.keySet()) {
      setCustomProperty(key, customProperties.get(key));
    }
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }
}
