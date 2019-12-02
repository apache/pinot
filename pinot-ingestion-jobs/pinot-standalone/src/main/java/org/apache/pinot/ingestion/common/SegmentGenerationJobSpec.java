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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;


public class SegmentGenerationJobSpec {

  String jobType;

  String _inputDirURI;

  String _includeFileNamePattern;

  String _excludeFileNamePattern;

  String _outputDirURI;

  boolean _overwriteOutput;

  List<PinotFSSpec> _pinotFSSpecs;

  TableSpec _tableSpec;

  RecordReaderSpec _recordReaderSpec;

  SegmentNameGeneratorSpec _segmentNameGeneratorSpec;

  PinotClusterSpec[] _pinotClusterSpecs;

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

  public static void main(String[] args)
      throws Exception {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    Yaml yaml = new Yaml(options);

    Reader reader;
    reader = new BufferedReader(new FileReader("/Users/kishoreg/Documents/testJobSpec.yaml"));
    SegmentGenerationJobSpec loadedSpec = yaml.loadAs(reader, SegmentGenerationJobSpec.class);
    System.out.println("loadedSpec = " + loadedSpec);

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();

    StringWriter sw = new StringWriter();
    yaml.dump(loadedSpec, sw);
    System.out.println("dump = " + sw.toString());
  }
}


