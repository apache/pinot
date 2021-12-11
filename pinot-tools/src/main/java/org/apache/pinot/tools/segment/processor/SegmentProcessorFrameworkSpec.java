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

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Container for all spec related to Segment Processor Framework required for running via pinot-admin
 */
public class SegmentProcessorFrameworkSpec {

  private String _inputSegmentsDir;
  private String _outputSegmentsDir;
  private String _tableConfigFile;
  private String _schemaFile;

  private TimeHandlerConfig _timeHandlerConfig;
  private List<PartitionerConfig> _partitionerConfigs;
  private MergeType _mergeType;
  private Map<String, AggregationFunctionType> _aggregationTypes;
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

  public TimeHandlerConfig getTimeHandlerConfig() {
    return _timeHandlerConfig;
  }

  public void setTimeHandlerConfig(TimeHandlerConfig timeHandlerConfig) {
    _timeHandlerConfig = timeHandlerConfig;
  }

  public List<PartitionerConfig> getPartitionerConfigs() {
    return _partitionerConfigs;
  }

  public void setPartitionerConfigs(List<PartitionerConfig> partitionerConfigs) {
    _partitionerConfigs = partitionerConfigs;
  }

  public MergeType getMergeType() {
    return _mergeType;
  }

  public void setMergeType(MergeType mergeType) {
    _mergeType = mergeType;
  }

  public Map<String, AggregationFunctionType> getAggregationTypes() {
    return _aggregationTypes;
  }

  public void setAggregationTypes(Map<String, AggregationFunctionType> aggregationTypes) {
    _aggregationTypes = aggregationTypes;
  }

  public SegmentConfig getSegmentConfig() {
    return _segmentConfig;
  }

  public void setSegmentConfig(SegmentConfig segmentConfig) {
    _segmentConfig = segmentConfig;
  }
}
