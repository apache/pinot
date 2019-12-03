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


/**
 * PinotFSSpec defines how to initialize a PinotFS for given scheme.
 *
 */
public class PinotFSSpec {

  /**
   * Scheme used to identify a PinotFS.
   */
  String _scheme;

  /**
   * Class name used to create the PinotFS instance.
   */
  String _className;

  /**
   * Configs used to init the PinotFS instances.
   */
  Map<String, String> _configs;

  public String getScheme() {
    return _scheme;
  }

  public void setScheme(String scheme) {
    _scheme = scheme;
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
}
