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
package org.apache.pinot.tools.segment.processor;

import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;


/**
 * Container for all spec related to Segment Processor Framework required for running via pinot-admin
 */
public class SegmentProcessorFrameworkSpec {

  private String _inputSegmentsDir;
  private String _outputSegmentsDir;
  private String _tableConfigFile;
  private String _schemaFile;

  private RecordTransformerConfig _recordTransformerConfig;
  private RecordFilterConfig _recordFilterConfig;
  private PartitionerConfig _partitionerConfig;
  private CollectorConfig _collectorConfig;
  private SegmentConfig _segmentConfig;

  public String getInputSegmentsDir() {
    return _inputSegmentsDir;
  }

  public void setInputSegmentsDir(String inputSegmentsDir) {
    _inputSegmentsDir = inputSegmentsDir;
  }

  public String getOutputSegmentsDir() {
    return _outputSegmentsDir;
  }

  public void setOutputSegmentsDir(String outputSegmentsDir) {
    _outputSegmentsDir = outputSegmentsDir;
  }

  public String getTableConfigFile() {
    return _tableConfigFile;
  }

  public void setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
  }

  public String getSchemaFile() {
    return _schemaFile;
  }

  public void setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
  }

  public RecordTransformerConfig getRecordTransformerConfig() {
    return _recordTransformerConfig;
  }

  public void setRecordTransformerConfig(RecordTransformerConfig recordTransformerConfig) {
    _recordTransformerConfig = recordTransformerConfig;
  }

  public RecordFilterConfig getRecordFilterConfig() {
    return _recordFilterConfig;
  }

  public void setRecordFilterConfig(RecordFilterConfig recordFilterConfig) {
    _recordFilterConfig = recordFilterConfig;
  }

  public PartitionerConfig getPartitionerConfig() {
    return _partitionerConfig;
  }

  public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
    _partitionerConfig = partitionerConfig;
  }

  public CollectorConfig getCollectorConfig() {
    return _collectorConfig;
  }

  public void setCollectorConfig(CollectorConfig collectorConfig) {
    _collectorConfig = collectorConfig;
  }

  public SegmentConfig getSegmentConfig() {
    return _segmentConfig;
  }

  public void setSegmentConfig(SegmentConfig segmentConfig) {
    _segmentConfig = segmentConfig;
  }
}
