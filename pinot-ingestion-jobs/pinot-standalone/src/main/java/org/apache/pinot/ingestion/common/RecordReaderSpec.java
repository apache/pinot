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

import java.util.Map;


public class RecordReaderSpec {

  String _dataFormat;

  String _className;

  String _configClassName;

  Map<String, String> _configs;

  public String getDataFormat() {
    return _dataFormat;
  }

  public void setDataFormat(String dataFormat) {
    _dataFormat = dataFormat;
  }

  public String getClassName() {
    return _className;
  }

  public void setClassName(String className) {
    _className = className;
  }

  public Map<String, String> getConfigs() {
    return _configs;
  }

  public void setConfigs(Map<String, String> configs) {
    _configs = configs;
  }

  public String getConfigClassName() {
    return _configClassName;
  }

  public void setConfigClassName(String configClassName) {
    _configClassName = configClassName;
  }
}
