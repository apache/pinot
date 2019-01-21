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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pinot.common.data.PinotObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;

public class JSONObject implements PinotObject {
  private static ObjectMapper _MAPPER = new ObjectMapper();
  private JsonNode _jsonNode;

  @Override
  public void init(byte[] bytes) {
    try {
      _jsonNode = _MAPPER.readTree(bytes);
    } catch (IOException e) {
      _jsonNode = JsonNodeFactory.instance.objectNode();
    }
  }

  @Override
  public byte[] toBytes() {
    try {
      return _MAPPER.writeValueAsBytes(_jsonNode);
    } catch (JsonProcessingException e) {
      return "{}".getBytes();
    }
  }

  @Override
  public List<String> getPropertyNames() {
    List<String> fields = Lists.newArrayList();
    // TODO: Add support to iterate recursively
    Iterator<String> iterator = _jsonNode.fieldNames();
    while (iterator.hasNext()) {
      String fieldName = (String) iterator.next();
      fields.add(fieldName);
    }
    return fields;
  }

  @Override
  public Object getProperty(String fieldName) {
    JsonNode jsonNode = _jsonNode.get(fieldName);
    if (jsonNode.isArray()) {
      Iterator<JsonNode> iterator = jsonNode.iterator();
      List<String> list = new ArrayList<String>();
      while (iterator.hasNext()) {
        list.add(iterator.next().asText());
      }
      return list;
    } else {
      return jsonNode.asText();
    }
  }

}
