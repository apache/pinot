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
package org.apache.pinot.ingestion.common;

import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class SegmentGenerationTaskSpec {

  TableConfig _tableConfig;

  Schema _schema;

  RecordReaderSpec _recordReaderSpec;

  SegmentNameGeneratorSpec _segmentNameGeneratorSpec;

  String _inputFilePath;

  String _outputDirectoryPath;

  int _sequenceId;

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public void setTableConfig(TableConfig tableConfig) {
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
}
