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
package org.apache.pinot.common.data.objects;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.pinot.common.data.PinotObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class MapObject implements PinotObject {

  private static ObjectMapper _MAPPER = new ObjectMapper();
  Map<String, Object> _stringMap;

  @SuppressWarnings("unchecked")
  @Override
  public void init(byte[] bytes) {
    try {
      _stringMap = _MAPPER.readValue(bytes, Map.class);
    } catch (IOException e) {
      _stringMap = Collections.emptyMap();
    }
  }

  @Override
  public byte[] toBytes() {
    try {
      return _MAPPER.writeValueAsBytes(_stringMap);
    } catch (JsonProcessingException e) {
      return "{}".getBytes();
    }
  }

  @Override
  public List<String> getPropertyNames() {
    return Lists.newArrayList(_stringMap.keySet());
  }

  @Override
  public Object getProperty(String field) {
    return _stringMap.get(field);
  }

}
