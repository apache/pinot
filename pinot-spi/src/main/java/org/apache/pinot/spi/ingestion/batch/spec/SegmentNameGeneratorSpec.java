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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * SegmentNameGeneratorSpec defines how to init a SegmentNameGenerator.
 */
public class SegmentNameGeneratorSpec implements Serializable {

  /**
   * Current supported type is 'simple' and 'normalizedDate'.
   */
  private String _type = null;

  /**
   * Configs to init SegmentNameGenerator.
   */
  private Map<String, String> _configs = new HashMap<>();

  public String getType() {
    return _type;
  }

  public void setType(String type) {
    _type = type;
  }

  public Map<String, String> getConfigs() {
    return _configs;
  }

  public void setConfigs(Map<String, String> configs) {
    _configs = configs;
  }
}
