/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.rootcause.impl;

import java.util.List;
import java.util.Map;

/**
 * Class to keep configs of each individual external rca pipeline
 * outputName: output name for pipeline
 * inputNames: input names for pipeline
 * className: class name containing implementation for this pipeline
 * properties: map of property name and value, which are required by this pipeline for instantiation
 */
public class PipelineConfiguration {
  private String outputName;
  private String className;
  private List<String> inputNames;
  private Map<String, Object> properties;

  public String getOutputName() {
    return outputName;
  }
  public void setOutputName(String outputName) {
    this.outputName = outputName;
  }
  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }
  public Map<String, Object> getProperties() {
    return properties;
  }
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
  public List<String> getInputNames() {
    return inputNames;
  }
  public void setInputNames(List<String> inputNames) {
    this.inputNames = inputNames;
  }
}
