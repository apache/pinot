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
package org.apache.pinot.spi.config;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * A helix-agnostic record that mirrors ZNRecord's data structure (id, simpleFields, mapFields).
 * Config classes in pinot-spi use this for serialization/deserialization, while bridge utilities
 * in pinot-common handle conversion between ConfigRecord and ZNRecord.
 */
public class ConfigRecord {
  private final String _id;
  private final Map<String, String> _simpleFields;
  private final Map<String, Map<String, String>> _mapFields;

  public ConfigRecord(String id, Map<String, String> simpleFields,
      Map<String, Map<String, String>> mapFields) {
    _id = id;
    _simpleFields = simpleFields != null ? simpleFields : Collections.emptyMap();
    _mapFields = mapFields != null ? mapFields : Collections.emptyMap();
  }

  public ConfigRecord(String id, Map<String, String> simpleFields) {
    this(id, simpleFields, Collections.emptyMap());
  }

  public String getId() {
    return _id;
  }

  public Map<String, String> getSimpleFields() {
    return _simpleFields;
  }

  @Nullable
  public String getSimpleField(String key) {
    return _simpleFields.get(key);
  }

  public Map<String, Map<String, String>> getMapFields() {
    return _mapFields;
  }

  @Nullable
  public Map<String, String> getMapField(String key) {
    return _mapFields.get(key);
  }
}
