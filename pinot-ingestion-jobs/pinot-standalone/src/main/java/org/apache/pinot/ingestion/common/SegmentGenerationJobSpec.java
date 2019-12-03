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

import java.util.List;


/**
 * SegmentGenerationJobSpec defines all the required information in order to kick off a Pinot data ingestion job.
 *
 */
public class SegmentGenerationJobSpec {

  /**
   * Supported job types are:
   *  'SegmentCreation'
   *  'SegmentTarPush'
   *  'SegmentUriPush'
   *  'SegmentCreationAndTarPush'
   *  'SegmentCreationAndUriPush'
   */
  String jobType;

  /**
   * Root directory of input data, expected to have scheme configured in PinotFS.
   */
  String _inputDirURI;

  /**
   * include file name pattern, supported glob pattern.
   *    'glob:*.avro' will include all avro files just under the inputDirURI, not sub directories;
   *    'glob:**\/*.avro' will include all the avro files under inputDirURI recursively.
   */
  String _includeFileNamePattern;

  /**
   * exclude file name pattern, supported glob pattern.
   * Sample usage:
   *    'glob:*.avro' will exclude all avro files just under the inputDirURI, not sub directories;
   *    'glob:**\/*.avro' will exclude all the avro files under inputDirURI recursively.
   */
  String _excludeFileNamePattern;

  /**
   * Root directory of output data, expected to have scheme configured in PinotFS.
   */
  String _outputDirURI;

  /**
   * Should orverwrite output segments if existed.
   */
  boolean _overwriteOutput;

  /**
   * All Pinot FS related specs
   */
  List<PinotFSSpec> _pinotFSSpecs;

  /**
   * Pinot Table Spec
   */
  TableSpec _tableSpec;

  /**
   * Data file RecordReader related spec
   */
  RecordReaderSpec _recordReaderSpec;

  /**
   * SegmentNameGenerator related spec
   */
  SegmentNameGeneratorSpec _segmentNameGeneratorSpec;

  /**
   * Pinot Cluster related specs
   */
  PinotClusterSpec[] _pinotClusterSpecs;

  /**
   * Segment Push job related spec
   */
  PushJobSpec _pushJobSpec;

  public String getJobType() {
    return jobType;
  }

  public void setJobType(String jobType) {
    this.jobType = jobType;
  }

  public String getInputDirURI() {
    return _inputDirURI;
  }

  public void setInputDirURI(String inputDirURI) {
    _inputDirURI = inputDirURI;
  }

  public String getIncludeFileNamePattern() {
    return _includeFileNamePattern;
  }

  public void setIncludeFileNamePattern(String includeFileNamePattern) {
    _includeFileNamePattern = includeFileNamePattern;
  }

  public String getExcludeFileNamePattern() {
    return _excludeFileNamePattern;
  }

  public void setExcludeFileNamePattern(String excludeFileNamePattern) {
    _excludeFileNamePattern = excludeFileNamePattern;
  }

  public String getOutputDirURI() {
    return _outputDirURI;
  }

  public void setOutputDirURI(String outputDirURI) {
    _outputDirURI = outputDirURI;
  }

  public boolean isOverwriteOutput() {
    return _overwriteOutput;
  }

  public void setOverwriteOutput(boolean overwriteOutput) {
    _overwriteOutput = overwriteOutput;
  }

  public List<PinotFSSpec> getPinotFSSpecs() {
    return _pinotFSSpecs;
  }

  public void setPinotFSSpecs(List<PinotFSSpec> pinotFSSpecs) {
    _pinotFSSpecs = pinotFSSpecs;
  }

  public TableSpec getTableSpec() {
    return _tableSpec;
  }

  public void setTableSpec(TableSpec tableSpec) {
    _tableSpec = tableSpec;
  }

  public RecordReaderSpec getRecordReaderSpec() {
    return _recordReaderSpec;
  }

  public void setRecordReaderSpec(RecordReaderSpec recordReaderSpec) {
    _recordReaderSpec = recordReaderSpec;
  }

  public PinotClusterSpec[] getPinotClusterSpecs() {
    return _pinotClusterSpecs;
  }

  public void setPinotClusterSpecs(PinotClusterSpec[] pinotClusterSpecs) {
    _pinotClusterSpecs = pinotClusterSpecs;
  }

  public SegmentNameGeneratorSpec getSegmentNameGeneratorSpec() {
    return _segmentNameGeneratorSpec;
  }

  public void setSegmentNameGeneratorSpec(SegmentNameGeneratorSpec segmentNameGeneratorSpec) {
    _segmentNameGeneratorSpec = segmentNameGeneratorSpec;
  }

  public PushJobSpec getPushJobSpec() {
    return _pushJobSpec;
  }

  public void setPushJobSpec(PushJobSpec pushJobSpec) {
    _pushJobSpec = pushJobSpec;
  }
}


